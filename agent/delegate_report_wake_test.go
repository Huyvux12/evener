package agent

import (
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"primeradiant.com/evener/agent/internal/agenttest"
	"primeradiant.com/evener/llm"
)

// A child sub-delegate's report must wake a parked non-root parent (#6330):
// the parent spawns its children, ends its turn with them live, and the first
// child's report is delivered while the second worker still holds the
// parent's attention. That delivery alone must carry the report to the
// parent's next model request the moment the hold clears — no delegate_send,
// no manual drive, nothing but the wake the delivery itself fired. The second
// child stays live until the test releases it, so the report cannot ride the
// parent's own drain and only the report's wake can carry the consumption.
//
// The refusal-visibility half of the fix is pinned separately, by
// TestDelegateAttentionDriveRefusalNamesTheHoldingPredicate: while this
// test's hold stands, the parked lane's generation is still open
// (its drain holds it open), so a drive pass through that window is the
// scan's by-design skip, not a refusal, and warning on it would be noise.
func TestChildReportWakesAParkedParentDelegate(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		adapter := newMessagingAdapter("COORD-BRIEF", "WORKER-A", "WORKER-B")
		// parked opens once the coordinator has ended its turn with both
		// workers live; holdB keeps the second worker's report back so the
		// first report's wake cannot ride the parent's own final drain.
		parked := make(chan struct{})
		holdB := make(chan struct{})
		presented := make(chan string, 1)
		adapter.script("root",
			func(llm.Request) llm.Response {
				return delegateToolResponse("coord", "COORD-BRIEF: start two worker delegates, then end your turn and wait for them.")
			},
			func(llm.Request) llm.Response { return agenttest.FinalResponse("waiting on the coordinator") },
		)
		adapter.script("COORD-BRIEF",
			func(llm.Request) llm.Response {
				return delegateToolResponse("workerA", "WORKER-A: do the work, then report.")
			},
			func(llm.Request) llm.Response {
				return delegateToolResponse("workerB", "WORKER-B: take your time, then report.")
			},
			func(llm.Request) llm.Response { return agenttest.FinalResponse("waiting on both workers") },
			func(req llm.Request) llm.Response {
				select {
				case presented <- messagesText(req.Messages):
				default:
				}
				return agenttest.FinalResponse("noted the reports")
			},
		)
		adapter.script("WORKER-A",
			func(llm.Request) llm.Response {
				awaitSignal(t, parked, "the coordinator to park")
				return agenttest.FinalResponse("WORKER_A_REPORT: alpha is done")
			},
		)
		adapter.script("WORKER-B",
			func(llm.Request) llm.Response {
				awaitSignal(t, holdB, "the test to release the second worker")
				return agenttest.FinalResponse("WORKER_B_REPORT: beta is done")
			},
		)
		root := newMessagingRoot(t, adapter, nil)
		serveSupervisionRootWakes(t, root)
		// TRIPWIRE: every answer is scripted in process; only a hang reaches it.
		ctx := tripwireCtx(t)
		if _, err := root.ProcessInput(ctx, "start", nil); err != nil {
			t.Fatalf("ProcessInput: %v", err)
		}
		// The coordinator's third request ends its turn; once the bubble is
		// quiet it is parked with both workers live underneath it.
		adapter.waitForRequests(t, "COORD-BRIEF", 3)
		requireSoleChildIdle(t, root)
		close(parked)
		// The first worker reports while the second still runs.
		adapter.waitForRequests(t, "WORKER-A", 1)
		synctest.Wait()
		// Release the hold: the first report's own wake must carry the
		// parent's next turn. Nothing else in this test drives the parent.
		close(holdB)
		select {
		case text := <-presented:
			if !strings.Contains(text, `kind="report"`) || !strings.Contains(text, "WORKER_A_REPORT") {
				t.Fatalf("the coordinator's next turn does not present the first worker's report:\n%s", text)
			}
		// TRIPWIRE: every answer is scripted in process; only a hang reaches it.
		case <-time.After(30 * time.Second):
			t.Fatal("the parked coordinator was never woken by its worker's report")
		}
		root.Close()
	})
}
