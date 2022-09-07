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

	for _, w := range config.Workers {
		_worker, err := worker.New(w)
		available := _worker.IsAvailable()
		if err != nil || available != true {
			log.Println("Failed to initialize worker", w.Name)
		}
		m.workers[w.Name] = _worker
	}

	m.startTime = ""
	m.currentTimeStep = 0
	m.currentNumJobsRunning = 0
	m.running = true

	return nil
}

func (m *Manager) Start() error {
	m.startTime = time.Now().Format("YYYY-MM-DD_HH:MM")

	// TODO: Convert to go routine!

	// main loop
	for {
		t0 := time.Now()

		// Poll
		for _, w := range m.workers {
			_, err := w.Stats()
			if err != nil {
				log.Println("Error updating stats for {}", w.Name)
			}
		}

		// Inference

		// Assign Job(s)

		// update time step or stop
		if m.currentTimeStep >= m.maxTimeStep {
			break
		} else if time.Since(t0) < m.stepSize {
			// wait for timestep to finish
		} else {
			m.currentTimeStep += 1
		}
	}

	return nil
}
