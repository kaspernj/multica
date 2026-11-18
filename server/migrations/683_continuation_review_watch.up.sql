-- Platform-armed continuation-review watchers.
--
-- A card assigned to an agent needs two watchers: 'forward' wakes the reviewer
-- when the assignee's run ends, 'mirror' wakes the assignee when the reviewer
-- comments. Nothing in the server produced them, so coverage came from a
-- host-side script somebody ran by hand; every card created after that single
-- run had no reviewer wired at all, and a card could be implemented, merged and
-- still never reach review.
--
-- The pair stays an ordinary pair of event rules, so capture, dispatch, runaway
-- protection and the timeline all apply unchanged. review_watch marks the rows
-- as platform-owned and bounds an issue to one rule of each kind.
ALTER TABLE issue_wakeup
 ADD COLUMN IF NOT EXISTS review_watch text CHECK (review_watch IN ('forward','mirror'));

CREATE UNIQUE INDEX IF NOT EXISTS issue_wakeup_review_forward_idx ON issue_wakeup (issue_id) WHERE review_watch='forward';
CREATE UNIQUE INDEX IF NOT EXISTS issue_wakeup_review_mirror_idx  ON issue_wakeup (issue_id) WHERE review_watch='mirror';

-- The pair is written for every agent-assigned issue, so it must not compete
-- for the 32-per-issue and 1000-per-workspace budget people and agents share:
-- a workspace already at that cap (measured: 999/1000) would arm nothing, which
-- is exactly the failure this change removes. Same body as 555, with the
-- review_watch exemption added to the early return and to neither count.
CREATE OR REPLACE FUNCTION guard_issue_wakeup_capacity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT NEW.enabled OR NEW.system_rule IS NOT NULL OR NEW.review_watch IS NOT NULL THEN RETURN NEW; END IF;
 IF TG_OP='UPDATE' AND OLD.enabled AND OLD.issue_id=NEW.issue_id AND OLD.workspace_id=NEW.workspace_id THEN RETURN NEW; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('issue-wakeup-capacity:'||NEW.workspace_id::text,0));
 IF (SELECT count(*) FROM (SELECT 1 FROM issue_wakeup WHERE workspace_id=NEW.workspace_id AND issue_id=NEW.issue_id AND enabled AND system_rule IS NULL AND review_watch IS NULL AND id<>NEW.id LIMIT 32) slots)>=32 THEN
  RAISE EXCEPTION 'An issue can have at most 32 enabled wakeups' USING ERRCODE='23514',CONSTRAINT='issue_wakeup_active_limit';
 END IF;
 IF (SELECT count(*) FROM (SELECT 1 FROM issue_wakeup WHERE workspace_id=NEW.workspace_id AND enabled AND system_rule IS NULL AND review_watch IS NULL AND id<>NEW.id LIMIT 1000) slots)>=1000 THEN
  RAISE EXCEPTION 'A workspace can have at most 1000 enabled wakeups' USING ERRCODE='23514',CONSTRAINT='issue_wakeup_active_limit';
 END IF;
 RETURN NEW;
END $$;
