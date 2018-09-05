package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.

	var wg sync.WaitGroup

	var taskChain = make(chan int)

	var taskArgs DoTaskArgs
	taskArgs.JobName = jobName
	taskArgs.NumOtherPhase = n_other
	taskArgs.Phase = phase

	go func() {
		for i := 0; i < ntasks; i++ {
			wg.Add(1)
			taskChain <- i
		}
		wg.Wait()
		close(taskChain)
	}()

	for i := range taskChain {
		worker := <-registerChan

		taskArgs.TaskNumber = i

		if phase == mapPhase {
			taskArgs.File = mapFiles[i]
		}

		go func(worker string, taskArgs DoTaskArgs) {
			if call(worker, "Worker.DoTask", &taskArgs, nil) {
				wg.Done()

				registerChan <- worker
			} else {
				fmt.Printf("Shedule: assign %s task %v to %s failed", phase, taskArgs.TaskNumber, worker)
				taskChain <- taskArgs.TaskNumber
			}
		}(worker, taskArgs)
	}

	fmt.Printf("Schedule: %v done\n", phase)
}
