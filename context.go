package libfat

import (
	"context"
	"errors"
)

// cancellationCheckInterval is how many units of work a cancellable operation
// performs between checks of its context.
//
// A check is cheap but not free, and the operations that take one - a tree walk,
// an orphan sweep - do very little per unit. Checking every unit would cost more
// than the work being paced. The counter that drives it spans the whole
// operation rather than resetting per directory or per cluster run, so a walk
// over a million small directories stays interruptible, and it is tested before
// it advances so a context that is already cancelled is caught on the first unit
// rather than the thousandth.
const cancellationCheckInterval = 1024

var (
	// ErrNilContext reports a nil context passed to a cancellable entry point. A
	// nil context is an error rather than a silent context.Background(): a caller
	// who forgot to thread one through has an interruptible operation that cannot
	// be interrupted, and silence hides that.
	ErrNilContext = errors.New("context is nil")

	// ErrNilCallback reports a nil function passed to a walk.
	ErrNilCallback = errors.New("callback is nil")
)

// scanProgress paces cancellation checks across one whole operation. It is a
// single counter shared by every phase of that operation, which is what keeps
// the pacing rule from drifting between phases: an orphan scan checks its
// reachability pre-pass, its cluster sweep and its continuation-run gather
// against the same counter rather than three that each start again at zero.
type scanProgress struct {
	ctx     context.Context
	counter uint64
}

// check reports the context's error every cancellationCheckInterval calls. The
// counter is tested before it advances, so the first call always checks.
func (p *scanProgress) check() error {
	if p.counter%cancellationCheckInterval == 0 {
		if err := p.ctx.Err(); err != nil {
			return err
		}
	}
	p.counter++
	return nil
}
