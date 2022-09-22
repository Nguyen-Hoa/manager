package manager

import (
	"encoding/json"
	"errors"
	"log"
	"sync"
	"time"

	logger "github.com/Nguyen-Hoa/csvlogger"
	profile "github.com/Nguyen-Hoa/profile"
	worker "github.com/Nguyen-Hoa/worker"
)

type Manager struct {
	// config
	logging      bool
	verbose      bool
	debug        bool
	maxTimeStep  int
	stepSize     time.Duration
	HasPredictor bool

	// models
	predictor *DNN

	// status
	workers               map[string]*worker.BaseWorker
	startTime             string
	currentTimeStep       int
	currentNumJobsRunning int
	running               bool
	JobQueue              []Job

	// loggers
	baseLogPath string
	statsLogger logger.Logger
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
	BaseLogPath string                `json:"baseLogPath"`
}

func (m *Manager) Init(config ManagerConfig) error {
	m.logging = config.Logging
	m.verbose = config.Verbose
	m.debug = config.Debug
	m.maxTimeStep = config.MaxTimeStep
	m.stepSize = time.Duration(config.StepSize) * time.Second
	m.HasPredictor = false

	m.baseLogPath = config.BaseLogPath
	statsLogger, err := logger.NewLogger(m.baseLogPath, "stats.csv")
	if err != nil {
		return err
	}
	m.statsLogger = statsLogger

	// TODO: How to create init generic predictor?
	if config.ModelPath != "" {
		predictor := DNN{}
		predictor.Init(config.ModelPath)
		m.predictor = &predictor
		m.HasPredictor = true
	}

	m.workers = make(map[string]*worker.BaseWorker)
	m.JobQueue = config.JobQueue
	for _, w := range config.Workers {
		_worker, err := worker.New(w)
		available := _worker.IsAvailable()
		if err != nil {
			log.Println("Failed to initialize worker", w.Name)
			log.Println(err)
		} else if !available {
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
			m.Log(w)
		}(w)
	}
	pollWaitGroup.Wait()

	// Inference
	if m.HasPredictor {
		log.Println("Inference...")
		for _, w := range m.workers {
			m.predictor.Predict(w)
		}
	}

	// Assign Job(s)
	if len(m.JobQueue) > 0 {
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
			// target.StartJob(newJob.Image, newJob.Cmd)
			log.Printf("Would have started %s at %s", newJob.Image, target.Name)
		}
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
		return Job{}, errors.New("no jobs remaining")
	}

	job := m.JobQueue[0]
	m.JobQueue = m.JobQueue[1:]
	return job, nil
}

/* Testing ONLY*/
func (m *Manager) findWorker() (*worker.BaseWorker, error) {
	w := m.workers["kimchi"]
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

func (m *Manager) Log(w *worker.BaseWorker) error {
	stats_ := w.GetStats()
	marsh_stats, err := json.Marshal(stats_)
	if err != nil {
		log.Print(err)
		return err
	}
	stats := profile.DNN_params{}
	if err := json.Unmarshal(marsh_stats, &stats); err != nil {
		log.Print(err)
		return err
	}

	m.statsLogger.Add(stats)
	return nil
}
