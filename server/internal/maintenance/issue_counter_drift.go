package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

const IssueCounterDriftType = "workspace_issue_counter_drift"

// driftDetailLimit caps the per-workspace detail a job keeps in its progress and
// result JSON. The counters stay exact past the cap, so a self-host holding
// thousands of workspaces reports the same totals without a jsonb column that
// grows without bound.
const driftDetailLimit = 100

// IssueCounterDrift reports workspaces whose issue_counter sits below the highest
// number their issue rows carry. That gap is what makes a create collide: the
// counter hands out a number another row already holds, and the write fails on
// uq_issue_workspace_number. Nothing in the request path can tell an operator
// which workspace is behind, so this is the read side of the same fault.
type IssueCounterDrift struct{}

func (IssueCounterDrift) Type() string { return IssueCounterDriftType }
func (IssueCounterDrift) Version() int { return 1 }

type counterDriftParameters struct {
	Repair bool `json:"repair"`
}

func (IssueCounterDrift) Validate(scope string, dryRun bool, raw json.RawMessage) (json.RawMessage, error) {
	var p counterDriftParameters
	if err := decode(raw, &p); err != nil {
		return nil, err
	}
	if scope != "database" {
		return nil, fmt.Errorf("%w: this processor requires scope_key=database", ErrInvalid)
	}
	// Scanning is a report. Moving a counter is the part an operator has to ask
	// for by name, so a non-dry run that did not say repair is refused instead of
	// quietly writing nothing.
	if !dryRun && !p.Repair {
		return nil, fmt.Errorf("%w: pass repair=true to move counters, or keep this job a dry-run report", ErrInvalid)
	}
	return asJSON(p), nil
}

func (IssueCounterDrift) Preflight(ctx context.Context, tx pgx.Tx) error {
	var present bool
	if err := tx.QueryRow(ctx, "SELECT to_regclass('workspace') IS NOT NULL").Scan(&present); err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("workspace is not on the search path")
	}
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_attribute
 WHERE attrelid=to_regclass('workspace') AND attname='issue_counter' AND attnum>0)`).Scan(&present); err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("workspace.issue_counter is missing; this processor reads it")
	}
	if err := tx.QueryRow(ctx, "SELECT to_regclass('issue') IS NOT NULL").Scan(&present); err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("issue is not on the search path")
	}
	// The constraint is what makes the lag observable at all: without it no create
	// ever collides on a number, so a report here would be a guess about a workspace
	// that could hold any number.
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_index i
 WHERE i.indrelid=to_regclass('issue') AND i.indexrelid=to_regclass($1) AND i.indisunique AND i.indisvalid)`,
		service.IssueNumberConstraint).Scan(&present); err != nil {
		return err
	}
	if !present {
		return fmt.Errorf("unique index %s is missing or invalid", service.IssueNumberConstraint)
	}
	return nil
}

type counterDriftCheckpoint struct {
	AfterID string `json:"after_id"`
}

type counterDriftWorkspace struct {
	WorkspaceID string `json:"workspace_id"`
	Counter     int    `json:"issue_counter"`
	MaxNumber   int    `json:"max_number"`
	Gap         int    `json:"gap"`
}

type counterDriftProgress struct {
	WorkspacesScanned  int                     `json:"workspaces_scanned"`
	WorkspacesDrifted  int                     `json:"workspaces_drifted"`
	WorkspacesRepaired int                     `json:"workspaces_repaired"`
	MaxGap             int                     `json:"max_gap"`
	Drift              []counterDriftWorkspace `json:"drift"`
	DetailsTruncated   bool                    `json:"details_truncated"`
}

type counterDriftReport struct {
	DryRun    bool      `json:"dry_run"`
	Repair    bool      `json:"repair"`
	CheckedAt time.Time `json:"checked_at"`
	counterDriftProgress
}

func (IssueCounterDrift) Step(ctx context.Context, tx pgx.Tx, j Job) (json.RawMessage, json.RawMessage, json.RawMessage, bool, error) {
	var cp counterDriftCheckpoint
	var progress counterDriftProgress
	var p counterDriftParameters
	if err := decode(j.Checkpoint, &cp); err != nil {
		return nil, nil, nil, false, err
	}
	if err := decode(j.Progress, &progress); err != nil {
		return nil, nil, nil, false, err
	}
	if err := decode(j.Parameters, &p); err != nil {
		return nil, nil, nil, false, err
	}

	// Same page shape as the status processor: one indexed page per request, and the
	// checkpoint advances over examined workspace ids rather than over the drifted
	// ones, so a mostly healthy database costs one index scan per call. The max
	// number is read per workspace through idx_issue_workspace_number, which is the
	// index the collision lookup already uses.
	query := `SELECT w.id::text, w.issue_counter,
    COALESCE((SELECT max(i.number) FROM issue i WHERE i.workspace_id = w.id), 0)::int
 FROM workspace w`
	args := []any{j.Options.BatchSize}
	if cp.AfterID != "" {
		query += " WHERE w.id > $2::uuid"
		args = append(args, cp.AfterID)
	}
	// Qualify the UUID column so ORDER BY resolves to the index, not to the
	// SELECT's id::text output.
	query += " ORDER BY w.id LIMIT $1"
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return nil, nil, nil, false, err
	}
	var lastID string
	type driftTarget struct {
		id        string
		counter   int
		maxNumber int
		gap       int
	}
	var targets []driftTarget
	for rows.Next() {
		var id string
		var counter, maxNumber int
		if err = rows.Scan(&id, &counter, &maxNumber); err != nil {
			break
		}
		lastID = id
		progress.WorkspacesScanned++
		if counter >= maxNumber {
			continue
		}
		progress.WorkspacesDrifted++
		gap := maxNumber - counter
		if gap > progress.MaxGap {
			progress.MaxGap = gap
		}
		if len(progress.Drift) < driftDetailLimit {
			progress.Drift = append(progress.Drift, counterDriftWorkspace{id, counter, maxNumber, gap})
		} else {
			progress.DetailsTruncated = true
		}
		if !j.DryRun {
			targets = append(targets, driftTarget{id, counter, maxNumber, gap})
		}
	}
	rowErr := rows.Err()
	rows.Close()
	if err != nil {
		return nil, nil, nil, false, err
	}
	if rowErr != nil {
		return nil, nil, nil, false, rowErr
	}
	// Repairs happen after the page is closed: pgconn serves one operation at a
	// time, so writing while its own result set is still open fails the batch. The
	// repair set is every drifted workspace in the page, not the capped report
	// detail. HealIssueCounter is the repair the merged collision handler uses, so
	// report and repair cannot disagree about what "behind" means, and its WHERE
	// guard returns no row for a counter that is no longer behind — a workspace the
	// request path already healed is left alone rather than failing the job.
	if !j.DryRun {
		for _, target := range targets {
			uid, e := util.ParseUUID(target.id)
			if e != nil {
				return nil, nil, nil, false, e
			}
			row, e := db.New(tx).HealIssueCounter(ctx, uid)
			if errors.Is(e, pgx.ErrNoRows) {
				continue
			}
			if e != nil {
				return nil, nil, nil, false, e
			}
			progress.WorkspacesRepaired++
			slog.Info("maintenance counter repair", "job_id", j.ID, "workspace_id", target.id,
				"previous_counter", row.PreviousCounter, "issue_counter", row.IssueCounter, "max_number", row.MaxNumber)
		}
	}
	if lastID == "" {
		report := counterDriftReport{DryRun: j.DryRun, Repair: p.Repair, CheckedAt: time.Now().UTC()}
		report.counterDriftProgress = progress
		return asJSON(cp), asJSON(progress), asJSON(report), true, nil
	}
	cp.AfterID = lastID
	return asJSON(cp), asJSON(progress), j.Result, false, nil
}
