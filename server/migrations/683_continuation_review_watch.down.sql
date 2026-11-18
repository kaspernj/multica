-- Restore the 555 guard before dropping the column it would reference.
CREATE OR REPLACE FUNCTION guard_issue_wakeup_capacity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT NEW.enabled OR NEW.system_rule IS NOT NULL THEN RETURN NEW; END IF;
 IF TG_OP='UPDATE' AND OLD.enabled AND OLD.issue_id=NEW.issue_id AND OLD.workspace_id=NEW.workspace_id THEN RETURN NEW; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended('issue-wakeup-capacity:'||NEW.workspace_id::text,0));
 IF (SELECT count(*) FROM (SELECT 1 FROM issue_wakeup WHERE workspace_id=NEW.workspace_id AND issue_id=NEW.issue_id AND enabled AND system_rule IS NULL AND id<>NEW.id LIMIT 32) slots)>=32 THEN
  RAISE EXCEPTION 'An issue can have at most 32 enabled wakeups' USING ERRCODE='23514',CONSTRAINT='issue_wakeup_active_limit';
 END IF;
 IF (SELECT count(*) FROM (SELECT 1 FROM issue_wakeup WHERE workspace_id=NEW.workspace_id AND enabled AND system_rule IS NULL AND id<>NEW.id LIMIT 1000) slots)>=1000 THEN
  RAISE EXCEPTION 'A workspace can have at most 1000 enabled wakeups' USING ERRCODE='23514',CONSTRAINT='issue_wakeup_active_limit';
 END IF;
 RETURN NEW;
END $$;

-- Rules already armed stay as ordinary event rules: they still wake the reviewer
-- and the assignee, they just lose the platform marker and rejoin the shared
-- capacity count. Disable/drain them before rolling this back if the workspace
-- is close to the cap.
DROP INDEX IF EXISTS issue_wakeup_review_forward_idx;
DROP INDEX IF EXISTS issue_wakeup_review_mirror_idx;
ALTER TABLE issue_wakeup DROP COLUMN IF EXISTS review_watch;
