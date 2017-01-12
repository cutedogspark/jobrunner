package jobrunner

import (
	"gopkg.in/robfig/cron.v2"
)

const DEFAULT_JOB_POOL_SIZE = 100

var (
	// Singleton instance of the underlying job scheduler.
	MainCron *cron.Cron

	// This limits the number of jobs allowed to run concurrently.
	workPermits chan struct{}

	// Is a single job allowed to run concurrently with itself?
	selfConcurrent bool
)

func Start(v ...int) {
	MainCron = cron.New()

	if len(v) > 0 {
		if v[0] > 0 {
			workPermits = make(chan struct{}, v[0])
		} else {
			workPermits = make(chan struct{}, DEFAULT_JOB_POOL_SIZE)
		}
	}

	if len(v) > 1 {
		if v[1] > 0 {
			selfConcurrent = true
		} else {
			selfConcurrent = false
		}
	}
	MainCron.Start()
}
