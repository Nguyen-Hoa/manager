package manager

import (
	"errors"
	"log"
	"time"

	worker "github.com/Nguyen-Hoa/worker"
)

type Manager struct {
	// config
	logging     bool
	verbose     bool
	debug       bool
	maxTimeStep int
	stepSize    time.Duration

	// status
	workers               map[string]*worker.BaseWorker
	startTime             string
	currentTimeStep       int
	currentNumJobsRunning int
	running               bool
	JobQueue              []Job
}

type Job struct {
	Image string   `json:"image"`
	Cmd   []string `json:"cmd"`
}

type ManagerConfig struct {
	Logging     bool                  `json:"logging"`
	Verbose     bool                  `json:"verbose"`
	Debug       bool                  `json:"debug"`
	MaxTimeStep int                   `json:"maxTimeStep"`
	StepSize    int                   `json:"stepSize"`
	Workers     []worker.WorkerConfig `json:"workers"`
	JobQueue    []Job                 `json:"jobs"`
}

func (m *Manager) Init(config ManagerConfig) error {
	m.logging = config.Logging
	m.verbose = config.Verbose
	m.debug = config.Debug
	m.maxTimeStep = config.MaxTimeStep
	m.stepSize = time.Duration(config.StepSize) * time.Second

	m.workers = make(map[string]*worker.BaseWorker)
	for _, w := range config.Workers {
		_worker, err := worker.New(w)
		available := _worker.IsAvailable()
		if err != nil {
			log.Println("Failed to initialize worker", w.Name)
			log.Println(err)
		} else if available != true {
			log.Println("Worker unavailable, ensure worker server is running.", w.Name)
		}
		m.workers[w.Name] = _worker
	}

	m.startTime = ""
	m.currentTimeStep = 0
	m.currentNumJobsRunning = 0
	m.running = true

	return nil
}

func step(done chan bool, t0 time.Time, m *Manager) error {

	// Poll
	// log.Println("Poll...")
	// for _, w := range m.workers {
	// 	_, err := w.Stats()
	// 	if err != nil {
	// 		log.Println("Error updating stats for %s", w.Name)
	// 	}
	// }

	// Inference
	// log.Println("Inference...")

	// Assign Job(s)
	log.Println("Scheduling")

	newJob, err := m.requestJob()
	if err != nil {
		log.Println(err)
		done <- true
		m.currentTimeStep += 1
		return err
	}

	target, err := m.findWorker()
	if err != nil {
		log.Println(err)
		done <- true
		m.currentTimeStep += 1
		return err
	}

	target.StartJob(newJob.Image, newJob.Cmd)

	// Wait for end of time step
	if time.Since(t0) < m.stepSize {
		time.Sleep(m.stepSize - time.Since(t0))
	}
	m.currentTimeStep += 1
	done <- true
	return nil
}

func (m *Manager) requestJob() (Job, error) {
	if len(m.JobQueue) <= 0 {
		return Job{}, errors.New("No jobs remaining")
	}

	job := m.JobQueue[0]
	m.JobQueue = m.JobQueue[1:]
	return job, nil
}

/* Testing ONLY*/
func (m *Manager) findWorker() (worker.BaseWorker, error) {
	w := m.workers["kimchi"]
	return *w, nil
}

func (m *Manager) Start() error {
	m.startTime = time.Now().Format("YYYY-MM-DD_HH:MM")

	// main loop
	for {
		done := make(chan bool, 1)
		t0 := time.Now()
		go step(done, t0, m)
		<-done

		if m.currentTimeStep >= m.maxTimeStep {
			break
		}
	}

	return nil
}
