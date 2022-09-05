package manager

import (
	"log"
	"time"

	worker "github.com/Nguyen-Hoa/worker"
)

type Manager struct {
	// config
	maxTimeStep int
	logging     bool
	verbose     bool
	debug       bool

	// status
	workers               map[string]*worker.BaseWorker
	startTime             string
	currentTimeStep       int
	currentNumJobsRunning int
	running               bool
}

type ManagerConfig struct {
	MaxTimeStep int                   `json:"maxTimeStep"`
	Logging     bool                  `json:"logging"`
	Verbose     bool                  `json:"verbose"`
	Debug       bool                  `json:"debug"`
	Workers     []worker.WorkerConfig `json:"workers"`
}

func (m *Manager) Init(config ManagerConfig) error {
	m.maxTimeStep = config.MaxTimeStep
	m.logging = config.Logging
	m.verbose = config.Verbose
	m.debug = config.Debug

	for _, w := range config.Workers {
		_worker, err := worker.New(w)
		if err != nil {
			m.workers[w.Name] = _worker
		}

	}

	m.startTime = ""
	m.currentTimeStep = 0
	m.currentNumJobsRunning = 0
	m.running = true

	return nil
}

func (m *Manager) Start() error {
	m.startTime = time.Now().Format("YYYY-MM-DD_HH:MM")

	// main loop
	for {
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
		} else {
			m.currentTimeStep += 1
		}
	}

	return nil
}
