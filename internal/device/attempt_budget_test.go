package device

import "testing"

func TestAttemptPermitCannotReleaseAnotherHolder(t *testing.T) {
	budget, err := NewAttemptBudget(1)
	if err != nil {
		t.Fatal(err)
	}
	first, ok := budget.TryAcquire()
	if !ok {
		t.Fatal("no initial permit")
	}
	first()
	second, ok := budget.TryAcquire()
	if !ok {
		t.Fatal("released permit unavailable")
	}
	defer second()
	first() // A delayed duplicate cleanup must not release the second holder.
	if budget.InUse() != 1 {
		t.Fatal("duplicate release stole another holder's permit")
	}
	if release, ok := budget.TryAcquire(); ok {
		release()
		t.Fatal("concurrency cap bypassed")
	}
	if budget.Denied() != 1 {
		t.Fatal("missing denied acquisition")
	}
}
