package manager

import (
	"errors"
	"log"
	"sync"
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

	// models
	predictor *DNN

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
	ModelPath   string                `json:"modelPath"`
	Workers     []worker.WorkerConfig `json:"workers"`
	JobQueue    []Job                 `json:"jobs"`
}

func (m *Manager) Init(config ManagerConfig) error {
	m.logging = config.Logging
	m.verbose = config.Verbose
	m.debug = config.Debug
	m.maxTimeStep = config.MaxTimeStep
	m.stepSize = time.Duration(config.StepSize) * time.Second

	// TODO: How to create init generic predictor?
	predictor := DNN{}
	predictor.Init(config.ModelPath)
	m.predictor = &predictor

	m.workers = make(map[string]*worker.BaseWorker)
	m.JobQueue = config.JobQueue
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
	log.Println("Poll...")
	var pollWaitGroup sync.WaitGroup
	for _, w := range m.workers {
		pollWaitGroup.Add(1)
		go func(w *worker.BaseWorker) {
			defer pollWaitGroup.Done()
			_, err := w.Stats()
			if err != nil {
				log.Printf("Error updating stats for %s", w.Name)
			}
		}(w)
	}
	pollWaitGroup.Wait()

	// Inference
	log.Println("Inference...")
	for _, w := range m.workers {
		m.predictor.Predict(w)
	}

	// Assign Job(s)
	log.Println("Scheduling")

	newJob, errJob := m.requestJob()
	if errJob != nil {
		log.Println(errJob)
	}

	target, errWorker := m.findWorker()
	if errWorker != nil {
		log.Println(errWorker)
	}

	if errJob == nil && errWorker == nil {
		target.StartJob(newJob.Image, newJob.Cmd)
	}

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
func (m *Manager) findWorker() (*worker.BaseWorker, error) {
	w := m.workers["kraken"]
	return w, nil
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
