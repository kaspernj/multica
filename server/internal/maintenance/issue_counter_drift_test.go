package maintenance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/testutil"
)

// counterFixture reuses the package's private test schema and adds the two
// tables this processor reads, from the migrations that actually created them:
// 001 makes workspace and issue, 020 adds issue.number, workspace.issue_counter
// and the uq_issue_workspace_number constraint whose violation is what a
// counter lag looks like from the application side.
func counterFixture(t *testing.T) (*pgxpool.Pool, *Service) {
	t.Helper()
	pool, _ := fixture(t)
	for _, name := range []string{"001_init", "020_issue_number"} {
		body, err := os.ReadFile(filepath.Join("..", "..", "migrations", name+".up.sql"))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(context.Background(), string(body)); err != nil {
			t.Fatalf("migration %s: %v", name, err)
		}
	}
	return pool, NewService(pool, IssueCounterDrift{})
}

// seedCounter writes a workspace holding issues numbered 1..maxNumber while its
// issue_counter sits at counter, so the gap between the two is exactly what the
// test is about. counter above maxNumber is the healthy case where issues have
// been deleted; counter below it is the drift that makes the next create collide.
func seedCounter(t *testing.T, pool *pgxpool.Pool, counter, maxNumber int) string {
	t.Helper()
	workspaceID, userID := uuid.NewString(), uuid.NewString()
	fx := testutil.New(pool, workspaceID, userID)
	handle := "counter-" + workspaceID[:8]
	fx.User(t, handle, handle+"@example.test")
	fx.Workspace(t, handle, handle, testutil.Cols{"id": workspaceID, "issue_counter": counter})
	for number := 1; number <= maxNumber; number++ {
		fx.Issue(t, fmt.Sprintf("issue %d", number), testutil.Cols{"workspace_id": workspaceID, "number": number})
	}
	return workspaceID
}

func counterRequest(dry bool, parameters any) CreateRequest {
	return CreateRequest{Type: IssueCounterDriftType, Version: 1, Scope: "database", IdempotencyKey: uuid.NewString(),
		DryRun: &dry, Options: Options{BatchSize: 2, DelayMS: 1, LockTimeoutMS: 50, StatementTimeoutMS: 1000},
		Parameters: asJSON(parameters)}
}

func createCounter(t *testing.T, s *Service, dry bool, parameters any) Job {
	t.Helper()
	job, err := s.Create(context.Background(), counterRequest(dry, parameters))
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func counterReport(t *testing.T, raw json.RawMessage) counterDriftReport {
	t.Helper()
	var report counterDriftReport
	if err := json.Unmarshal(raw, &report); err != nil {
		t.Fatalf("decode result %s: %v", raw, err)
	}
	return report
}

func counterValue(t *testing.T, pool *pgxpool.Pool, workspaceID string) int {
	t.Helper()
	var counter int
	if err := pool.QueryRow(context.Background(), "SELECT issue_counter FROM workspace WHERE id=$1::uuid", workspaceID).Scan(&counter); err != nil {
		t.Fatal(err)
	}
	return counter
}

// The board fault behind STOE-1102: workspace.issue_counter 4 while the
// workspace holds an issue numbered 7, so every create hands out a taken number.
func TestIssueCounterDriftReportsWorkspaceBelowMaxIssueNumber(t *testing.T) {
	pool, s := counterFixture(t)
	drifted := seedCounter(t, pool, 4, 7)
	healthy := seedCounter(t, pool, 7, 7)
	empty := seedCounter(t, pool, 0, 0)

	job := finish(t, s, createCounter(t, s, true, struct{}{}))
	if job.Status != "completed" {
		t.Fatalf("job status %s: %s", job.Status, job.LastError)
	}
	report := counterReport(t, job.Result)
	if !report.DryRun || report.Repair {
		t.Fatalf("a report job must stay a dry run: %+v", report)
	}
	if report.WorkspacesScanned != 3 || report.WorkspacesDrifted != 1 || report.MaxGap != 3 {
		t.Fatalf("scanned %d, drifted %d, max gap %d; want 3, 1, 3",
			report.WorkspacesScanned, report.WorkspacesDrifted, report.MaxGap)
	}
	if len(report.Drift) != 1 {
		t.Fatalf("drift detail %+v; want the drifted workspace only", report.Drift)
	}
	got := report.Drift[0]
	if got.WorkspaceID != drifted || got.Counter != 4 || got.MaxNumber != 7 || got.Gap != 3 {
		t.Fatalf("drift detail %+v; want workspace %s at counter 4 below max 7 with gap 3", got, drifted)
	}
	for _, id := range []string{drifted, healthy, empty} {
		want := map[string]int{drifted: 4, healthy: 7, empty: 0}[id]
		if value := counterValue(t, pool, id); value != want {
			t.Fatalf("dry run wrote workspace %s counter to %d, want %d", id, value, want)
		}
	}
}

// repair=true is the only way a counter moves, and only a workspace that is
// genuinely behind its highest number is touched.
func TestIssueCounterDriftRepairsOnlyWorkspacesBehindTheirMaxNumber(t *testing.T) {
	pool, s := counterFixture(t)
	drifted := seedCounter(t, pool, 4, 7)
	ahead := seedCounter(t, pool, 9, 7)

	job := finish(t, s, createCounter(t, s, false, counterDriftParameters{Repair: true}))
	if job.Status != "completed" {
		t.Fatalf("job status %s: %s", job.Status, job.LastError)
	}
	report := counterReport(t, job.Result)
	if report.WorkspacesRepaired != 1 || report.WorkspacesDrifted != 1 {
		t.Fatalf("repaired %d of %d drifted; want 1 of 1", report.WorkspacesRepaired, report.WorkspacesDrifted)
	}
	if value := counterValue(t, pool, drifted); value != 7 {
		t.Fatalf("repaired counter is %d, want MAX(issue.number) 7", value)
	}
	if value := counterValue(t, pool, ahead); value != 9 {
		t.Fatalf("counter ahead of its numbers was rewritten to %d, want 9", value)
	}
}

func TestIssueCounterDriftRequiresRepairForNonDryRun(t *testing.T) {
	_, s := counterFixture(t)
	_, err := s.Create(context.Background(), counterRequest(false, struct{}{}))
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("non-dry-run without repair: %v; want ErrInvalid", err)
	}
}

func TestIssueCounterDriftRejectsWorkspaceScope(t *testing.T) {
	_, s := counterFixture(t)
	_, err := s.Create(context.Background(), CreateRequest{
		Type: IssueCounterDriftType, Version: 1, Scope: "workspace:" + uuid.NewString(),
		IdempotencyKey: uuid.NewString(), Options: counterRequest(true, struct{}{}).Options,
		Parameters: asJSON(struct{}{}),
	})
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("workspace scope: %v; want ErrInvalid", err)
	}
}

// The counter is only observable while the constraint that makes a collision
// possible is installed; without it a lag is invisible and a repair pointless.
func TestIssueCounterDriftPreflightRejectsMissingNumberConstraint(t *testing.T) {
	pool, s := counterFixture(t)
	seedCounter(t, pool, 4, 7)
	if _, err := pool.Exec(context.Background(), "ALTER TABLE issue DROP CONSTRAINT "+service.IssueNumberConstraint); err != nil {
		t.Fatal(err)
	}
	_, err := s.Create(context.Background(), counterRequest(true, struct{}{}))
	if err == nil || !strings.Contains(err.Error(), service.IssueNumberConstraint) {
		t.Fatalf("preflight with no %s: %v; want a refusal naming the constraint", service.IssueNumberConstraint, err)
	}
}
