package manager

import (
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
}

type ManagerConfig struct {
	Logging     bool                  `json:"logging"`
	Verbose     bool                  `json:"verbose"`
	Debug       bool                  `json:"debug"`
	MaxTimeStep int                   `json:"maxTimeStep"`
	StepSize    int                   `json:"stepSize"`
	Workers     []worker.WorkerConfig `json:"workers"`
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

func step(done chan bool, t0 time.Time, m *Manager) {

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
	// log.Println("Scheduling")

	// Wait for end of time step
	if time.Since(t0) < m.stepSize {
		time.Sleep(m.stepSize - time.Since(t0))
	}
	m.currentTimeStep += 1
	done <- true
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
