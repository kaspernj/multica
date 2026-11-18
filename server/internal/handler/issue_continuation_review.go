package handler

import (
	"context"
	"log/slog"

	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// armContinuationReview wires the reviewer/assignee watcher pair for one issue
// after a write committed. It is best-effort by design: the pair is derived
// state, and failing to arm it must not fail the request that moved the
// assignee. The backfill pass re-arms any open agent-assigned issue that has no
// pair, so a logged failure is retried rather than lost.
func (h *Handler) armContinuationReview(ctx context.Context, issue db.Issue) {
	if _, err := (&service.IssueWakeupService{Tasks: h.TaskService}).ArmContinuationReview(ctx, issue); err != nil {
		slog.Warn("continuation review arm failed", "issue_id", uuidToString(issue.ID), "error", err)
	}
}
