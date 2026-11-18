package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/pkg/eventcontract"
)

// A card whose assignee is an agent is watched by a pair of rules the server
// writes itself: 'forward' wakes the reviewer when the assignee's run ends,
// 'mirror' wakes the assignee when the reviewer comments. Only the pair closes
// the loop — a forward watcher alone reviews a card nobody then acts on, and a
// mirror alone has nothing to mirror.
//
// They are ordinary event rules, so capture, dispatch, runaway protection and
// the timeline all apply unchanged. review_watch marks them as platform-owned,
// bounds an issue to one rule of each kind, and exempts them from the capacity
// budget people and agents share.
const (
	ReviewWatchForward = "forward"
	ReviewWatchMirror  = "mirror"

	// WorkspaceSettingContinuationReview turns the pair off; only an explicit
	// false does. Reviewer names the reviewing agent; Excluded lists assignees
	// the platform must never watch (an agent whose runs are request-only, or an
	// assignee a reviewer must not chase).
	WorkspaceSettingContinuationReview         = "continuation_review"
	WorkspaceSettingContinuationReviewReviewer = "continuation_review_reviewer_agent_id"
	WorkspaceSettingContinuationReviewExcluded = "continuation_review_excluded_agent_ids"

	// reviewForwardMaxFires bounds one review loop on a card; reviewMirrorMaxFires
	// bounds the return half, which fires once per reviewer comment.
	reviewForwardMaxFires = 10
	reviewMirrorMaxFires  = 200
)

// ContinuationReviewDefault reports whether a workspace wants the pair and which
// agent reviews it. No reviewer named means nothing is armed: the platform has
// no sensible default reviewer, and arming a watcher that cannot run would burn
// a rule slot on every card for nothing.
func ContinuationReviewDefault(settings []byte) (bool, pgtype.UUID) {
	var values map[string]json.RawMessage
	if len(settings) == 0 || json.Unmarshal(settings, &values) != nil {
		return false, pgtype.UUID{}
	}
	var reviewer string
	_ = json.Unmarshal(values[WorkspaceSettingContinuationReviewReviewer], &reviewer)
	reviewerID, err := util.ParseUUID(strings.TrimSpace(reviewer))
	if err != nil {
		return false, pgtype.UUID{}
	}
	return string(values[WorkspaceSettingContinuationReview]) != "false", reviewerID
}

// continuationReviewExcluded is the workspace's do-not-watch list. An
// unparseable entry is skipped rather than dropping the whole list, so one bad
// id cannot silently disable coverage for every other assignee.
func continuationReviewExcluded(settings []byte) map[string]struct{} {
	var values map[string]json.RawMessage
	if len(settings) == 0 || json.Unmarshal(settings, &values) != nil {
		return nil
	}
	var ids []string
	if err := json.Unmarshal(values[WorkspaceSettingContinuationReviewExcluded], &ids); err != nil {
		return nil
	}
	out := make(map[string]struct{}, len(ids))
	for _, raw := range ids {
		id, err := util.ParseUUID(strings.TrimSpace(raw))
		if err != nil {
			continue
		}
		out[agentKey(id)] = struct{}{}
	}
	return out
}

// agentKey is the comparable form of an agent id: a pgtype.UUID parsed from text
// and one read back from the driver hold the same address but are not ==.
func agentKey(id pgtype.UUID) string {
	if !id.Valid {
		return ""
	}
	return util.UUIDToString(id)
}

// forwardReviewInstruction is what the reviewer reads when the assignee's run
// ends. It is the brief the manually-armed watchers on this board already carry,
// kept word for word: the reviewer classifies the stopped run and hands it back
// or re-routes it, and never takes the card, merges, deploys or writes code.
func forwardReviewInstruction(assigneeName string) string {
	return fmt.Sprintf("Continuation review of this card: the %s run terminated. Classify it and dispatch the assignee or re-route; "+
		"never take the card, never merge, never deploy, never write code.", assigneeName)
}

// mirrorReviewInstruction is the return half, and the reason a forward watcher
// alone is not coverage: the reviewer's comment has to reach the assignee. A
// reviewer comment that asks for nothing must not start a run that comments back,
// or the pair feeds itself, so the brief ends it silently instead.
func mirrorReviewInstruction(reviewerName string) string {
	return fmt.Sprintf("%s commented on this card. If their comment says no action is needed, end silently with no comment and no change. "+
		"Only act when they name a finding, a missing decision, or a blocked next step.", reviewerName)
}

// ArmContinuationReview registers the pair for one issue, or re-points the
// existing pair when the assignee changed. It is idempotent: the partial unique
// indexes make a second row of the same kind impossible, so a racing writer loses
// the insert and re-points the row that won. An issue whose assignee is not an
// armable agent (member, squad, unassigned, archived, no runtime, excluded, or
// the reviewer itself) keeps no pair, so a card moved off the coder pool does not
// leave a watcher dispatching a reviewer nobody asked for.
//
// It reports whether a watcher is in force afterwards, which is what the backfill
// counts to know when it is done.
func (s *IssueWakeupService) ArmContinuationReview(ctx context.Context, issue db.Issue) (bool, error) {
	ws, err := s.Tasks.Queries.GetWorkspace(ctx, issue.WorkspaceID)
	if err != nil {
		return false, err
	}
	enabled, reviewerID := ContinuationReviewDefault(ws.Settings)
	if !enabled {
		return false, nil
	}
	if !issue.AssigneeType.Valid || issue.AssigneeType.String != "agent" || !issue.AssigneeID.Valid {
		return false, s.retireContinuationReview(ctx, issue.ID)
	}
	assignee, err := s.Tasks.Queries.GetAgent(ctx, issue.AssigneeID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, s.retireContinuationReview(ctx, issue.ID)
	}
	if err != nil {
		return false, err
	}
	excluded := continuationReviewExcluded(ws.Settings)
	_, isExcluded := excluded[agentKey(assignee.ID)]
	if assignee.ArchivedAt.Valid || !assignee.RuntimeID.Valid || isExcluded || agentKey(assignee.ID) == agentKey(reviewerID) {
		return false, s.retireContinuationReview(ctx, issue.ID)
	}
	reviewer, err := s.Tasks.Queries.GetAgent(ctx, reviewerID)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if reviewer.ArchivedAt.Valid || !reviewer.RuntimeID.Valid {
		return false, nil
	}

	forwardText := forwardReviewInstruction(assignee.Name)
	mirrorText := mirrorReviewInstruction(reviewer.Name)

	// Each half needs a creator who may invoke the agent it wakes: dispatch
	// authorizes a rule through its creator and disables one whose creator cannot,
	// so arming without one would arm a rule that switches itself off on its first
	// dispatch. The agent's owner is the accountable human for what that agent runs.
	forwardCreator, reviewerUsable, err := s.reviewWatchCreator(ctx, issue.WorkspaceID, reviewer)
	if err != nil {
		return false, err
	}
	mirrorCreator, assigneeUsable, err := s.reviewWatchCreator(ctx, issue.WorkspaceID, assignee)
	if err != nil {
		return false, err
	}
	if !reviewerUsable || !assigneeUsable {
		return false, s.retireContinuationReview(ctx, issue.ID)
	}

	forward, err := s.Tasks.Queries.CreateReviewForwardRule(ctx, db.CreateReviewForwardRuleParams{
		ID:            dbid.NewV7(),
		WorkspaceID:   issue.WorkspaceID,
		IssueID:       issue.ID,
		AgentID:       reviewer.ID,
		CreatedBy:     forwardCreator,
		Instruction:   forwardText,
		EventTypes:    []string{eventcontract.EventTaskCompleted, eventcontract.EventTaskFailed},
		FilterAgentID: assignee.ID,
		MaxFires:      pgtype.Int4{Int32: reviewForwardMaxFires, Valid: true},
		Enabled:       true,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		forward, err = s.Tasks.Queries.UpdateReviewForwardRule(ctx, db.UpdateReviewForwardRuleParams{
			AgentID: reviewer.ID, CreatedBy: forwardCreator, FilterAgentID: assignee.ID,
			Instruction: forwardText, IssueID: issue.ID,
		})
	}
	if err != nil {
		return false, err
	}
	mirror, err := s.Tasks.Queries.CreateReviewMirrorRule(ctx, db.CreateReviewMirrorRuleParams{
		ID:            dbid.NewV7(),
		WorkspaceID:   issue.WorkspaceID,
		IssueID:       issue.ID,
		AgentID:       assignee.ID,
		CreatedBy:     mirrorCreator,
		Instruction:   mirrorText,
		EventTypes:    []string{eventcontract.EventCommentCreated},
		FilterActorID: reviewer.ID,
		MaxFires:      pgtype.Int4{Int32: reviewMirrorMaxFires, Valid: true},
		Enabled:       true,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		mirror, err = s.Tasks.Queries.UpdateReviewMirrorRule(ctx, db.UpdateReviewMirrorRuleParams{
			AgentID: assignee.ID, CreatedBy: mirrorCreator, FilterActorID: reviewer.ID,
			Instruction: mirrorText, IssueID: issue.ID,
		})
	}
	if err != nil {
		return false, err
	}
	slog.Debug("continuation review armed",
		"issue_id", util.UUIDToString(issue.ID),
		"forward_wakeup_id", util.UUIDToString(forward.ID),
		"mirror_wakeup_id", util.UUIDToString(mirror.ID))
	return true, nil
}

// retireContinuationReview deletes the pair when an issue no longer has an
// armable assignee. A forward rule left behind would wake the reviewer for a card
// they were never asked to review.
func (s *IssueWakeupService) retireContinuationReview(ctx context.Context, issueID pgtype.UUID) error {
	rows, err := s.Tasks.Queries.DeleteReviewWatchPair(ctx, issueID)
	if err != nil {
		return err
	}
	if rows > 0 {
		slog.Debug("continuation review retired", "issue_id", util.UUIDToString(issueID), "rules_removed", rows)
	}
	return nil
}

// reviewWatchCreator is the human a review watcher is armed under: the agent's
// owner, who must still be a workspace member able to invoke that agent. A rule
// whose creator cannot invoke its agent is disabled by dispatch's authorization,
// so an agent with no invocable owner gets no watcher rather than a dead one.
func (s *IssueWakeupService) reviewWatchCreator(ctx context.Context, workspaceID pgtype.UUID, a db.Agent) (pgtype.UUID, bool, error) {
	if !a.OwnerID.Valid {
		return pgtype.UUID{}, false, nil
	}
	if _, err := s.Tasks.Queries.GetMemberByUserAndWorkspace(ctx, db.GetMemberByUserAndWorkspaceParams{UserID: a.OwnerID, WorkspaceID: workspaceID}); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return pgtype.UUID{}, false, nil
		}
		return pgtype.UUID{}, false, err
	}
	if !CanMemberInvokeAgent(ctx, s.Tasks.Queries, a, a.OwnerID, workspaceID) {
		return pgtype.UUID{}, false, nil
	}
	return a.OwnerID, true, nil
}

// BackfillContinuationReview arms open agent-assigned issues that predate the
// pair. It returns how many it armed; zero means every open issue has one, which
// is what retires the backfill pass.
func (s *IssueWakeupService) BackfillContinuationReview(ctx context.Context, limit int32) (int, error) {
	issues, err := s.Tasks.Queries.ListIssuesWithoutReviewWatch(ctx, limit)
	if err != nil {
		return 0, err
	}
	armed := 0
	for _, issue := range issues {
		ok, err := s.ArmContinuationReview(ctx, issue)
		if err != nil {
			slog.Warn("continuation review backfill: arming failed; will retry",
				"error", err, "issue_id", util.UUIDToString(issue.ID))
			continue
		}
		if ok {
			armed++
		}
	}
	return armed, nil
}
