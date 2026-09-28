package evmonly

import (
	"hash/fnv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOCCSchedulerSingleWorkerSweep(t *testing.T) {
	s := newOCCScheduler(3)

	task := s.nextTask()
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0}, task)
	// validationIdx (0) is not above 0: nothing to return, the sweep validates it.
	require.Equal(t, occTask{}, s.finishExecution(0, 0, true))

	task = s.nextTask()
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 0}, task, "validation is preferred once it lags execution")
	task, err := s.finishValidation(0, 0, false)
	require.NoError(t, err)
	require.Equal(t, occTask{}, task)

	for idx := 1; idx < 3; idx++ {
		task = s.nextTask()
		require.Equal(t, occTask{kind: occTaskExecute, txIdx: idx}, task)
		require.Equal(t, occTask{}, s.finishExecution(idx, 0, true))
		task = s.nextTask()
		require.Equal(t, occTask{kind: occTaskValidate, txIdx: idx}, task)
		task, err = s.finishValidation(idx, 0, false)
		require.NoError(t, err)
		require.Equal(t, occTask{}, task)
	}
	require.Equal(t, occTask{}, s.nextTask())
	require.True(t, s.isDone())
	require.True(t, s.allValidated())
	require.Zero(t, s.numActive.Load())
}

func TestOCCSchedulerValidationAbortReschedulesExecution(t *testing.T) {
	s := newOCCScheduler(2)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0}, s.nextTask())
	require.Equal(t, occTask{}, s.finishExecution(0, 0, true))
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 0}, s.nextTask())
	_, err := s.finishValidation(0, 0, false)
	require.NoError(t, err)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 1}, s.nextTask())
	require.Equal(t, occTask{}, s.finishExecution(1, 0, true))
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 1}, s.nextTask())

	require.True(t, s.tryValidationAbort(1, 0))
	require.False(t, s.tryValidationAbort(1, 0), "only one abort per incarnation")
	task, err := s.finishValidation(1, 0, true)
	require.NoError(t, err)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 1, incarnation: 1}, task, "execution passed the tx, so the re-execution is handed back")
	require.Equal(t, int64(2), s.validationIdx.Load())
	require.False(t, s.isDone())

	// No new locations: only the transaction itself is re-validated.
	task = s.finishExecution(1, 1, false)
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 1, incarnation: 1}, task)
	require.False(t, s.tryValidationAbort(1, 0), "stale incarnation cannot abort")
	task, err = s.finishValidation(1, 1, false)
	require.NoError(t, err)
	require.Equal(t, occTask{}, task)
	require.True(t, s.isDone())
	require.True(t, s.allValidated())
}

func TestOCCSchedulerAbortWithNewLocationsRewindsValidation(t *testing.T) {
	s := newOCCScheduler(3)
	for idx := range 3 {
		require.Equal(t, occTask{kind: occTaskExecute, txIdx: idx}, s.nextToExecute())
		require.Equal(t, occTask{}, s.finishExecution(idx, 0, true))
	}
	for idx := range 3 {
		require.Equal(t, occTask{kind: occTaskValidate, txIdx: idx}, s.nextTask())
		_, err := s.finishValidation(idx, 0, false)
		require.NoError(t, err)
	}
	require.True(t, s.allValidated())

	// A lower re-execution rewinds validation; the re-validation of tx0 then
	// fails, rewinding validation to 1, and tx0's re-execution with new writes
	// rewinds it to 0.
	s.decreaseValidationIdx(0)
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 0}, s.nextTask())
	require.True(t, s.tryValidationAbort(0, 0))
	require.False(t, s.allValidated())
	task, err := s.finishValidation(0, 0, true)
	require.NoError(t, err)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0, incarnation: 1}, task)
	require.Equal(t, int64(1), s.validationIdx.Load())
	require.Equal(t, occTask{}, s.finishExecution(0, 1, true))
	require.Equal(t, int64(0), s.validationIdx.Load())
	require.False(t, s.isDone())

	for idx := range 3 {
		task = s.nextTask()
		require.Equal(t, occTaskValidate, task.kind)
		require.Equal(t, idx, task.txIdx)
		_, err := s.finishValidation(task.txIdx, task.incarnation, false)
		require.NoError(t, err)
	}
	require.True(t, s.isDone())
	require.True(t, s.allValidated())
}

func TestOCCSchedulerDependencies(t *testing.T) {
	s := newOCCScheduler(3)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0}, s.nextTask())
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 1}, s.nextTask(), "tx0 is executing so its validation is skipped")
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 2}, s.nextTask())
	require.Equal(t, int64(3), s.numActive.Load())

	require.True(t, s.addDependency(2, 0))
	require.Equal(t, int64(2), s.numActive.Load())
	require.Equal(t, occTxAborting, s.txs[2].status)

	require.Equal(t, occTask{}, s.finishExecution(0, 0, true))
	require.Equal(t, occTxReady, s.txs[2].status, "dependents are woken")
	require.Equal(t, int64(2), s.executionIdx.Load(), "execution rewinds to the woken dependent")

	require.False(t, s.addDependency(1, 0), "tx0 already executed: caller retries")
	require.Equal(t, occTxExecuting, s.txs[1].status)
	require.Equal(t, occTask{}, s.finishExecution(1, 0, false))

	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 0}, s.nextTask())
	_, err := s.finishValidation(0, 0, false)
	require.NoError(t, err)
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 1}, s.nextTask())
	_, err = s.finishValidation(1, 0, false)
	require.NoError(t, err)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 2}, s.nextTask())
	require.Equal(t, occTask{}, s.finishExecution(2, 0, true))
	require.Equal(t, occTask{kind: occTaskValidate, txIdx: 2}, s.nextTask())
	_, err = s.finishValidation(2, 0, false)
	require.NoError(t, err)
	require.True(t, s.isDone())
	require.True(t, s.allValidated())
}

func TestOCCSchedulerIncarnationCap(t *testing.T) {
	s := newOCCScheduler(1)
	require.Equal(t, occTask{kind: occTaskExecute, txIdx: 0}, s.nextTask())
	s.txs[0].incarnation = occMaxTxIncarnations - 2
	require.Equal(t, occTask{}, s.finishExecution(0, occMaxTxIncarnations-2, true))
	task := s.nextTask()
	require.Equal(t, occTaskValidate, task.kind)
	require.True(t, s.tryValidationAbort(0, occMaxTxIncarnations-2))
	task, err := s.finishValidation(0, occMaxTxIncarnations-2, true)
	require.NoError(t, err)
	require.Equal(t, occMaxTxIncarnations-1, task.incarnation)
	require.Equal(t, occTask{}, s.finishExecution(0, occMaxTxIncarnations-1, true))
	task = s.nextTask()
	require.True(t, s.tryValidationAbort(0, occMaxTxIncarnations-1))
	_, err = s.finishValidation(0, occMaxTxIncarnations-1, true)
	require.ErrorIs(t, err, errOCCMaxIncarnation)
}

// TestOCCSchedulerConcurrentTermination drives the scheduler from several
// goroutines with a fake executor whose validations fail on a fixed pattern.
// It must terminate with every transaction validated.
func TestOCCSchedulerConcurrentTermination(t *testing.T) {
	const n, workers = 96, 8
	fails := func(idx, inc int) bool {
		if inc >= 3 {
			return false
		}
		h := fnv.New32a()
		h.Write([]byte{byte(idx), byte(idx >> 8), byte(inc)})
		return h.Sum32()%3 == 0
	}
	for round := 0; round < 20; round++ {
		s := newOCCScheduler(n)
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				var task occTask
				for !s.isDone() {
					if task.kind == occTaskNone {
						task = s.nextTask()
						continue
					}
					switch task.kind {
					case occTaskExecute:
						// Every other transaction pretends to hit an estimate of its
						// predecessor on its first incarnation.
						if task.incarnation == 0 && task.txIdx%2 == 1 && s.addDependency(task.txIdx, task.txIdx-1) {
							task = occTask{}
							continue
						}
						task = s.finishExecution(task.txIdx, task.incarnation, task.incarnation == 0)
					case occTaskValidate:
						aborted := fails(task.txIdx, task.incarnation) && s.tryValidationAbort(task.txIdx, task.incarnation)
						var err error
						task, err = s.finishValidation(task.txIdx, task.incarnation, aborted)
						require.NoError(t, err)
					}
				}
			}()
		}
		wg.Wait()
		require.True(t, s.allValidated(), "round %d", round)
		require.Zero(t, s.numActive.Load())
		for i := range s.txs {
			require.Equal(t, occTxValidated, s.txs[i].status, "tx %d", i)
		}
	}
}
