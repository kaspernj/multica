package service

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/eventcontract"
)

// The coverage these tests exist for: a card assigned to an agent had no reviewer
// wired by the server at all. The pair was armed by a host-side script somebody
// ran by hand, so every card created after that single run — and every card
// re-assigned after it — had nothing watching it, and could be implemented,
// merged and never reach review.

// reviewFixture is a workspace that named its reviewer, a coder agent, a reviewer
// agent and an issue assigned to the coder. settings is built per test because
// "who reviews", "the pair is off" and "never watch this agent" are all workspace
// settings, and the armer reads them rather than inventing a reviewer.
type reviewFixture struct {
	principalFixture
	svc      *IssueWakeupService
	issue    pgtype.UUID
	assignee pgtype.UUID
	reviewer pgtype.UUID
}

type reviewSettings func(reviewerID, assigneeID string) map[string]any

func defaultReviewSettings(reviewerID, _ string) map[string]any {
	return map[string]any{WorkspaceSettingContinuationReviewReviewer: reviewerID}
}

func reviewWatchFixture(t *testing.T, settings reviewSettings) reviewFixture {
	t.Helper()
	f, owner := newPrincipalFixture(t)
	if err := f.q.SeedIssueStatusEntries(context.Background(), parseTestUUID(t, f.WorkspaceID)); err != nil {
		t.Fatal(err)
	}
	n := principalSeq.Add(1)
	reviewerID := f.privateAgentOwnedBy(t, owner, fmt.Sprintf("review-reviewer-%d", n))
	assigneeID := f.privateAgentOwnedBy(t, owner, fmt.Sprintf("review-coder-%d", n))
	raw, err := json.Marshal(settings(reviewerID, assigneeID))
	if err != nil {
		t.Fatal(err)
	}
	f.Exec(t, "UPDATE workspace SET settings=$1::jsonb WHERE id=$2", string(raw), f.WorkspaceID)
	issueID := f.Issue(t, "continuation review", testutil.Cols{
		"assignee_type": "agent", "assignee_id": assigneeID, "status": "in_progress",
	})
	// Service-created data is cleaned before fixture owners, including receipts.
	f.Cleanup(t, "DELETE FROM issue_wakeup WHERE issue_id=$1", issueID)
	f.Cleanup(t, "DELETE FROM issue_wakeup_receipt WHERE wakeup_id IN (SELECT id FROM issue_wakeup WHERE issue_id=$1)", issueID)
	return reviewFixture{
		principalFixture: f,
		svc:              &IssueWakeupService{Tasks: f.svc.TaskSvc},
		issue:            parseTestUUID(t, issueID),
		assignee:         parseTestUUID(t, assigneeID),
		reviewer:         parseTestUUID(t, reviewerID),
	}
}

func (r reviewFixture) arm(t *testing.T) bool {
	t.Helper()
	issue, err := r.q.GetIssue(context.Background(), r.issue)
	if err != nil {
		t.Fatalf("load issue: %v", err)
	}
	armed, err := r.svc.ArmContinuationReview(context.Background(), issue)
	if err != nil {
		t.Fatalf("ArmContinuationReview: %v", err)
	}
	return armed
}

func (r reviewFixture) rule(t *testing.T, watch string) db.IssueWakeup {
	t.Helper()
	w, err := r.q.GetReviewWatchRule(context.Background(), db.GetReviewWatchRuleParams{
		IssueID: r.issue, ReviewWatch: pgtype.Text{String: watch, Valid: true},
	})
	if err != nil {
		t.Fatalf("no %s watcher on this issue: %v", watch, err)
	}
	return w
}

// pairCount is (forward, mirror). More than one of either is the duplicate-watch
// failure mode: a card would gain two forward watchers that both dispatch the
// reviewer.
func (r reviewFixture) pairCount(t *testing.T) (int, int) {
	t.Helper()
	return r.Count(t, "SELECT count(*) FROM issue_wakeup WHERE issue_id=$1 AND review_watch='forward'", r.issue),
		r.Count(t, "SELECT count(*) FROM issue_wakeup WHERE issue_id=$1 AND review_watch='mirror'", r.issue)
}

func (r reviewFixture) assertNoPair(t *testing.T) {
	t.Helper()
	if forward, mirror := r.pairCount(t); forward != 0 || mirror != 0 {
		t.Fatalf("armed %d forward and %d mirror watchers where nothing should be armed", forward, mirror)
	}
}

func TestContinuationReviewArmsForwardAndMirrorOnAgentAssignee(t *testing.T) {
	r := reviewWatchFixture(t, defaultReviewSettings)
	if !r.arm(t) {
		t.Fatal("arming an open issue assigned to an agent armed nothing")
	}
	forward, mirror := r.rule(t, ReviewWatchForward), r.rule(t, ReviewWatchMirror)
	if f, m := r.pairCount(t); f != 1 || m != 1 {
		t.Fatalf("arming wrote %d forward and %d mirror rules, not one of each", f, m)
	}

	// Forward: the reviewer wakes when the assignee's run ends.
	if util.UUIDToString(forward.AgentID) != util.UUIDToString(r.reviewer) {
		t.Fatalf("forward watcher wakes %s, not the reviewer", util.UUIDToString(forward.AgentID))
	}
	if !forward.FilterAgentID.Valid || util.UUIDToString(forward.FilterAgentID) != util.UUIDToString(r.assignee) {
		t.Fatalf("forward watcher is not filtered to the assignee: %+v", forward.FilterAgentID)
	}
	if forward.Kind != "event" || forward.Mode != "continuous" || !forward.Enabled {
		t.Fatalf("forward watcher is not an enabled continuous event rule: %+v", forward)
	}
	if !slices.Equal(forward.EventTypes, []string{eventcontract.EventTaskCompleted, eventcontract.EventTaskFailed}) {
		t.Fatalf("forward watcher subscribes %v, not the end of the assignee's run", forward.EventTypes)
	}
	if !forward.MaxFires.Valid || forward.MaxFires.Int32 != 10 {
		t.Fatalf("forward watcher max_fires=%v, want 10", forward.MaxFires)
	}
	if forward.ExpiresAt.Valid || forward.ExpirySeconds.Valid {
		t.Fatalf("forward watcher carries an expiry: %+v / %+v", forward.ExpiresAt, forward.ExpirySeconds)
	}
	if !forward.CreatedBy.Valid {
		t.Fatal("forward watcher has no creator, so dispatch would disable it on its first firing")
	}
	if !strings.Contains(forward.Instruction, "never take the card, never merge, never deploy, never write code") {
		t.Fatalf("forward watcher instruction is not the reviewer brief: %q", forward.Instruction)
	}

	// Mirror: the assignee wakes when the reviewer comments.
	if util.UUIDToString(mirror.AgentID) != util.UUIDToString(r.assignee) {
		t.Fatalf("mirror watcher wakes %s, not the assignee", util.UUIDToString(mirror.AgentID))
	}
	if mirror.FilterActorType.String != "agent" || util.UUIDToString(mirror.FilterActorID) != util.UUIDToString(r.reviewer) {
		t.Fatalf("mirror watcher is not filtered to the reviewer as an agent: %+v %+v", mirror.FilterActorType, mirror.FilterActorID)
	}
	if !slices.Equal(mirror.EventTypes, []string{eventcontract.EventCommentCreated}) {
		t.Fatalf("mirror watcher subscribes %v, not comment.created", mirror.EventTypes)
	}
	if !mirror.MaxFires.Valid || mirror.MaxFires.Int32 != 200 {
		t.Fatalf("mirror watcher max_fires=%v, want 200", mirror.MaxFires)
	}
	if mirror.ExpiresAt.Valid || mirror.ExpirySeconds.Valid {
		t.Fatalf("mirror watcher carries an expiry: %+v / %+v", mirror.ExpiresAt, mirror.ExpirySeconds)
	}
	if !strings.Contains(mirror.Instruction, "end silently with no comment and no change") {
		t.Fatalf("mirror watcher instruction does not stop a run from answering a no-action comment: %q", mirror.Instruction)
	}
}

func TestContinuationReviewPairIsIdempotentAndMovesWithTheAssignee(t *testing.T) {
	r := reviewWatchFixture(t, defaultReviewSettings)
	r.arm(t)
	forwardID, mirrorID := r.rule(t, ReviewWatchForward).ID, r.rule(t, ReviewWatchMirror).ID

	// Two writers arming the same card must not race a second pair.
	r.arm(t)
	r.arm(t)
	if f, m := r.pairCount(t); f != 1 || m != 1 {
		t.Fatalf("re-arming the same card produced %d forward and %d mirror rules", f, m)
	}

	other := r.privateAgentOwnedBy(t, r.UserID, fmt.Sprintf("review-coder-b-%d", principalSeq.Add(1)))
	r.Exec(t, "UPDATE issue SET assignee_id=$2 WHERE id=$1", r.issue, other)
	if !r.arm(t) {
		t.Fatal("moving the card to another agent armed nothing")
	}
	forward, mirror := r.rule(t, ReviewWatchForward), r.rule(t, ReviewWatchMirror)
	if f, m := r.pairCount(t); f != 1 || m != 1 {
		t.Fatalf("re-assignment produced %d forward and %d mirror rules; the stale pair was not replaced", f, m)
	}
	if forward.ID != forwardID || mirror.ID != mirrorID {
		t.Fatal("re-assignment replaced the rules instead of re-pointing them, losing their fire counts and history")
	}
	if util.UUIDToString(forward.FilterAgentID) != other {
		t.Fatalf("forward watcher still filters the old assignee: %s", util.UUIDToString(forward.FilterAgentID))
	}
	if util.UUIDToString(mirror.AgentID) != other || util.UUIDToString(mirror.FilterActorID) != util.UUIDToString(r.reviewer) {
		t.Fatalf("mirror watcher did not move to the new assignee: %+v", mirror)
	}
}

func TestContinuationReviewArmsNothingForAnAssigneeItMustNotWatch(t *testing.T) {
	cases := []struct {
		name     string
		settings reviewSettings
		prepare  func(t *testing.T, r reviewFixture)
	}{
		{
			name:     "the workspace named no reviewer",
			settings: func(string, string) map[string]any { return map[string]any{} },
		},
		{
			name: "the pair is switched off",
			settings: func(reviewerID, _ string) map[string]any {
				return map[string]any{WorkspaceSettingContinuationReviewReviewer: reviewerID, WorkspaceSettingContinuationReview: false}
			},
		},
		{
			name: "the assignee is on the do-not-watch list",
			settings: func(reviewerID, assigneeID string) map[string]any {
				return map[string]any{
					WorkspaceSettingContinuationReviewReviewer: reviewerID,
					WorkspaceSettingContinuationReviewExcluded: []string{assigneeID, "not-a-uuid"},
				}
			},
		},
		{
			name: "the assignee is the reviewer itself",
			settings: func(_, assigneeID string) map[string]any {
				return map[string]any{WorkspaceSettingContinuationReviewReviewer: assigneeID}
			},
		},
		{
			name:     "the assignee is a member",
			settings: defaultReviewSettings,
			prepare: func(t *testing.T, r reviewFixture) {
				r.Exec(t, "UPDATE issue SET assignee_type='member', assignee_id=$2 WHERE id=$1", r.issue, r.UserID)
			},
		},
		{
			name:     "the issue has no assignee",
			settings: defaultReviewSettings,
			prepare: func(t *testing.T, r reviewFixture) {
				r.Exec(t, "UPDATE issue SET assignee_type=NULL, assignee_id=NULL WHERE id=$1", r.issue)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := reviewWatchFixture(t, tc.settings)
			if tc.prepare != nil {
				tc.prepare(t, r)
			}
			if r.arm(t) {
				t.Fatal("arming reported a watcher where none should exist")
			}
			r.assertNoPair(t)
		})
	}
}

func TestContinuationReviewPairIsRetiredWhenTheCardLeavesTheCoderPool(t *testing.T) {
	r := reviewWatchFixture(t, defaultReviewSettings)
	if !r.arm(t) {
		t.Fatal("arming armed nothing")
	}
	if f, m := r.pairCount(t); f != 1 || m != 1 {
		t.Fatalf("expected a pair, got %d/%d", f, m)
	}
	// A card handed to a person keeps no watcher dispatching a reviewer nobody
	// asked for.
	r.Exec(t, "UPDATE issue SET assignee_type='member', assignee_id=$2 WHERE id=$1", r.issue, r.UserID)
	if r.arm(t) {
		t.Fatal("arming a member-assigned card reported a watcher")
	}
	r.assertNoPair(t)
}

func TestContinuationReviewPairIsArmedWhenTheWorkspaceIsAtItsWakeupCap(t *testing.T) {
	r := reviewWatchFixture(t, defaultReviewSettings)
	// The production state this change had to work around: the workspace already
	// at 1000 enabled non-system wakeups, with no agent-side reclaim path. 40
	// filler issues x 25 rules each fills it exactly.
	r.Exec(t, `INSERT INTO issue (id,workspace_id,title,status,priority,creator_type,creator_id,position,number)
		SELECT gen_random_uuid(),$1,'cap filler '||g,'todo','none','member',$2,0,900000+g FROM generate_series(1,40) g`,
		r.WorkspaceID, r.UserID)
	r.Cleanup(t, "DELETE FROM issue WHERE workspace_id=$1 AND title LIKE 'cap filler %'", r.WorkspaceID)
	r.Exec(t, `INSERT INTO issue_wakeup (id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,event_types,max_fires,enabled)
		SELECT gen_random_uuid(),i.workspace_id,i.id,$1,$2,'cap filler','event','continuous',ARRAY['comment.created'],10,true
		FROM issue i CROSS JOIN generate_series(1,25) s
		WHERE i.workspace_id=$3 AND i.title LIKE 'cap filler %'`,
		r.reviewer, r.UserID, r.WorkspaceID)
	r.Cleanup(t, "DELETE FROM issue_wakeup WHERE instruction='cap filler'")

	if n := r.Count(t, "SELECT count(*) FROM issue_wakeup WHERE workspace_id=$1 AND enabled AND system_rule IS NULL AND review_watch IS NULL", r.WorkspaceID); n != 1000 {
		t.Fatalf("fixture filled the workspace to %d ordinary enabled rules, not 1000", n)
	}
	// The cap is really in force here, so the pair passing through it proves the
	// exemption rather than a cap that was never reached.
	_, err := r.Pool.Exec(context.Background(),
		`INSERT INTO issue_wakeup (id,workspace_id,issue_id,agent_id,created_by,instruction,kind,mode,event_types,max_fires,enabled)
		 VALUES (gen_random_uuid(),$1,$2,$3,$4,'over cap','event','continuous',ARRAY['comment.created'],10,true)`,
		r.WorkspaceID, r.issue, r.reviewer, r.UserID)
	if err == nil || !strings.Contains(err.Error(), "at most 1000 enabled wakeups") {
		t.Fatalf("an ordinary rule was accepted at the workspace cap: %v", err)
	}

	if !r.arm(t) {
		t.Fatal("the pair was not armed in a workspace already at its wakeup cap")
	}
	if f, m := r.pairCount(t); f != 1 || m != 1 {
		t.Fatalf("expected the pair at the cap, got %d/%d", f, m)
	}
}

func TestContinuationReviewBackfillArmsOpenIssuesThatPredateThePair(t *testing.T) {
	r := reviewWatchFixture(t, defaultReviewSettings)
	r.assertNoPair(t)

	armed, err := r.svc.BackfillContinuationReview(context.Background(), 200)
	if err != nil {
		t.Fatalf("BackfillContinuationReview: %v", err)
	}
	if armed < 1 {
		t.Fatal("the backfill skipped the open agent-assigned issue in front of it")
	}
	forward, mirror := r.rule(t, ReviewWatchForward), r.rule(t, ReviewWatchMirror)
	if util.UUIDToString(forward.FilterAgentID) != util.UUIDToString(r.assignee) || util.UUIDToString(mirror.AgentID) != util.UUIDToString(r.assignee) {
		t.Fatalf("the backfill armed the pair against the wrong agent: %+v / %+v", forward, mirror)
	}

	// A pass that creates nothing is what retires the backfill: the pair is not
	// re-created on the next pass.
	armed, err = r.svc.BackfillContinuationReview(context.Background(), 200)
	if err != nil {
		t.Fatalf("second BackfillContinuationReview: %v", err)
	}
	if armed != 0 {
		t.Fatalf("the second pass armed %d issues; the first pass left candidates behind", armed)
	}
	if f, m := r.pairCount(t); f != 1 || m != 1 {
		t.Fatalf("the second pass changed the pair to %d/%d", f, m)
	}
}
