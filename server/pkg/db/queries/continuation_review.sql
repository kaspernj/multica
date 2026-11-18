-- name: GetReviewWatchRule :one
SELECT * FROM issue_wakeup WHERE issue_id = @issue_id AND review_watch = @review_watch;

-- name: CreateReviewForwardRule :one
-- Two writers arming the same issue keep the first row; the loser re-points it,
-- the same shape CreateSystemWakeup uses. created_by is the reviewing agent's
-- owner: dispatch authorizes a rule through its creator and disables one whose
-- creator cannot invoke the agent, so a NULL creator would arm a rule that
-- switches itself off on its first dispatch.
INSERT INTO issue_wakeup (id, workspace_id, issue_id, agent_id, created_by, instruction, kind, mode,
 event_types, filter_agent_id, max_fires, enabled, review_watch)
VALUES (@id, @workspace_id, @issue_id, @agent_id, @created_by, @instruction, 'event', 'continuous',
 @event_types, @filter_agent_id, @max_fires, @enabled, 'forward')
ON CONFLICT (issue_id) WHERE review_watch='forward' DO NOTHING
RETURNING *;

-- name: CreateReviewMirrorRule :one
INSERT INTO issue_wakeup (id, workspace_id, issue_id, agent_id, created_by, instruction, kind, mode,
 event_types, filter_actor_type, filter_actor_id, max_fires, enabled, review_watch)
VALUES (@id, @workspace_id, @issue_id, @agent_id, @created_by, @instruction, 'event', 'continuous',
 @event_types, 'agent', @filter_actor_id, @max_fires, @enabled, 'mirror')
ON CONFLICT (issue_id) WHERE review_watch='mirror' DO NOTHING
RETURNING *;

-- name: UpdateReviewForwardRule :one
-- Re-assignment re-points the watcher instead of replacing it: the rule keeps its
-- fire count, its history and its identity, so a card moved to a different coder
-- is still watched by one forward rule rather than two. A rule a runaway guard
-- paused stays paused — moving the assignee is not a way to clear that.
UPDATE issue_wakeup
SET agent_id = @agent_id, created_by = @created_by, filter_agent_id = @filter_agent_id,
 instruction = @instruction, updated_at = clock_timestamp(),
 enabled = paused_reason IS NULL,
 disabled_at = CASE WHEN paused_reason IS NULL THEN NULL ELSE disabled_at END
WHERE issue_id = @issue_id AND review_watch = 'forward'
RETURNING *;

-- name: UpdateReviewMirrorRule :one
UPDATE issue_wakeup
SET agent_id = @agent_id, created_by = @created_by, filter_actor_id = @filter_actor_id,
 instruction = @instruction, updated_at = clock_timestamp(),
 enabled = paused_reason IS NULL,
 disabled_at = CASE WHEN paused_reason IS NULL THEN NULL ELSE disabled_at END
WHERE issue_id = @issue_id AND review_watch = 'mirror'
RETURNING *;

-- name: DeleteReviewWatchPair :execrows
-- An issue that no longer has an armable assignee (member, squad, unassigned,
-- archived, excluded, or the reviewer itself) keeps no watcher pair.
DELETE FROM issue_wakeup WHERE issue_id = @issue_id AND review_watch IS NOT NULL;

-- name: ListIssuesWithoutReviewWatch :many
-- Open agent-assigned issues in a workspace that actually wants the pair, for
-- the one-time backfill. A workspace with no reviewer named is excluded here
-- rather than armed and skipped, so a pass that creates nothing really means
-- every candidate has its pair — otherwise those issues stay candidates forever
-- and the pass never retires.
SELECT i.* FROM issue i
JOIN workspace ws ON ws.id = i.workspace_id
WHERE i.assignee_type = 'agent' AND i.assignee_id IS NOT NULL
 AND ws.settings->>'continuation_review_reviewer_agent_id' IS NOT NULL
 AND COALESCE(ws.settings->>'continuation_review', 'true') <> 'false'
 AND i.status NOT IN ('done','cancelled')
 AND NOT EXISTS (SELECT 1 FROM issue_status s WHERE s.workspace_id = i.workspace_id AND s.key = i.status AND s.category IN ('done','closed'))
 AND NOT EXISTS (SELECT 1 FROM issue_wakeup watch WHERE watch.issue_id = i.id AND watch.review_watch = 'forward')
ORDER BY i.id
LIMIT @page_limit;
