package manager

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	logger "github.com/Nguyen-Hoa/csvlogger"
	job "github.com/Nguyen-Hoa/job"
	profile "github.com/Nguyen-Hoa/profile"
	scheduler "github.com/Nguyen-Hoa/scheduler"
	worker "github.com/Nguyen-Hoa/worker"

	"github.com/braintree/manners"
	"github.com/gin-gonic/gin"
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
	predictor Predictor
	scheduler scheduler.Scheduler

	// status
	workers               map[string]*worker.ManagerWorker
	startTime             string
	currentTimeStep       int
	currentNumJobsRunning int
	running               bool
	experimentDone        bool
	JobQueue              []job.Job
	APIPort               string
	RPCPort               string

	// loggers
	baseLogPath   string
	statsLogger   logger.Logger
	latencyLogger logger.Logger
}

type ManagerConfig struct {
	Logging                bool                  `json:"logging"`
	Verbose                bool                  `json:"verbose"`
	Debug                  bool                  `json:"debug"`
	MaxTimeStep            int                   `json:"maxTimeStep"`
	StepSize               int                   `json:"stepSize"`
	ModelPath              string                `json:"modelPath"`
	InferenceServerAddress string                `json:"inferenceServerAddress"`
	Workers                []worker.WorkerConfig `json:"workers"`
	JobQueue               []job.Job             `json:"jobs"`
	APIPort                string                `json:"apiPort"`
	RPCPort                string                `json:"rpcPort"`
	BaseLogPath            string                `json:"baseLogPath"`
}

type Latency struct {
	MachineID string
	Type      string
	Latency   time.Duration
}

func (m *Manager) Init(config ManagerConfig) error {
	m.logging = config.Logging
	m.verbose = config.Verbose
	m.debug = config.Debug
	m.maxTimeStep = config.MaxTimeStep
	m.stepSize = time.Duration(config.StepSize) * time.Second
	m.HasPredictor = false

	m.baseLogPath = filepath.Join(config.BaseLogPath, time.Now().Format("2006_01_02-15:04:05"))
	if err := os.MkdirAll(m.baseLogPath, os.ModePerm); err != nil {
		return err
	}

	statsLogger, err := logger.NewLogger(m.baseLogPath, "stats")
	if err != nil {
		return err
	}
	m.statsLogger = statsLogger

	latencyLogger, err := logger.NewLogger(m.baseLogPath, "latency")
	if err != nil {
		return err
	}
	m.latencyLogger = latencyLogger

	// TODO: How to create init generic predictor?
	if config.ModelPath != "" {
		predictor := DNN{}
		predictor.Init(config.ModelPath)
		m.predictor = &predictor
		m.HasPredictor = true
	}

	if config.InferenceServerAddress != "" {
		predictor := InferenceServer{}
		predictor.Init(config.InferenceServerAddress)
		m.predictor = &predictor
		m.HasPredictor = true
	}

	m.JobQueue = config.JobQueue
	if config.APIPort != "" {
		m.APIPort = config.APIPort
	} else {
		m.APIPort = ""
	}

	if config.RPCPort != "" {
		m.RPCPort = config.RPCPort
	} else {
		m.RPCPort = ""
	}

	m.workers = make(map[string]*worker.ManagerWorker)
	for _, w := range config.Workers {
		_worker, err := worker.New(w)
		available := _worker.IsAvailable()
		if err != nil {
			log.Println("Failed to initialize worker", w.Name)
			log.Println(err)
		} else if !available {
			log.Println("Worker unavailable, ensure worker server is running", w.Name)
		}

		if err := _worker.StartMeter(); err != nil {
			log.Println("Worker meter failure", w.Name)
			return err
		}
		m.workers[w.Name] = _worker
		log.Println("Initialized", w.Name)
	}

	m.startTime = ""
	m.currentTimeStep = 0
	m.currentNumJobsRunning = 0
	m.running = true
	m.experimentDone = false

	return nil
}

func parseConfig(configPath string) ManagerConfig {
	jsonFile, err := os.Open(configPath)
	if err != nil {
		log.Fatal("Failed to parse configuration file.")
	}
	defer jsonFile.Close()
	byteValue, _ := ioutil.ReadAll(jsonFile)
	var config ManagerConfig
	json.Unmarshal([]byte(byteValue), &config)
	return config
}

func NewManager(configPath string) (Manager, error) {
	m := Manager{}
	config := parseConfig(configPath)

	if err := m.Init(config); err != nil {
		log.Println("Failed to initialize manager!")
		log.Fatal(err)
		return m, err
	}
	log.Println("Initialized manager")
	return m, nil
}

func step(done chan bool, t0 time.Time, m *Manager) error {

	// Poll
	// log.Println("Poll...")
	var pollWaitGroup sync.WaitGroup
	var jobsRunning int = 0
	for _, w := range m.workers {
		pollWaitGroup.Add(1)
		go func(w *worker.ManagerWorker) {
			t0 := time.Now()
			defer pollWaitGroup.Done()
			_, err := w.Stats()
			tPoll := time.Since(t0)
			if err != nil {
				log.Printf("Error updating stats for %s", w.Name)
			}
			m.logStats(w)
			m.latencyLogger.Add(Latency{w.Name, "poll", tPoll})
			jobsRunning += len(w.RunningJobs)
		}(w)
	}
	pollWaitGroup.Wait()
	m.currentNumJobsRunning = jobsRunning

	// Inference
	if m.HasPredictor {
		// log.Println("Inference...")
		for _, w := range m.workers {
			t0 := time.Now()
			prediction, err := m.predictor.Predict(w)
			log.Print(prediction)
			w.LatestPredictedPower = prediction
			tInference := time.Since(t0)
			m.latencyLogger.Add(Latency{w.Name, "inference", tInference})
			if err != nil {
				log.Print(err)
				log.Printf("failed to predict for %s", w.Name)
			}
		}
	}

	// Assign Job(s)
	if len(m.JobQueue) > 0 {
		// log.Println("Scheduling")
		go func() {
			t0 := time.Now()
			if err := m.scheduler.Schedule(m.workers, m.JobQueue); err != nil {
				log.Print(err)
			}
			tSchedule := time.Since(t0)
			m.latencyLogger.Add(Latency{"manager", "schedule", tSchedule})
		}()
	}

	// Wait for end of time step
	if time.Since(t0) < m.stepSize {
		time.Sleep(m.stepSize - time.Since(t0))
	}
	done <- true
	return nil
}

func (m *Manager) schedule() error {
	t0 := time.Now()
	job := m.JobQueue[0]
	m.JobQueue = m.JobQueue[1:]
	target, err := m.findWorker()
	tSchedule := time.Since(t0)
	m.latencyLogger.Add(Latency{"manager", "schedule", tSchedule})
	if err != nil {
		return err
	}

	if err := target.StartJob(job.Image, job.Cmd, job.Duration); err != nil {
		return err
	}

	return nil
}

/* Testing ONLY*/
func (m *Manager) findWorker() (*worker.ManagerWorker, error) {
	w := m.workers["kimchi"]
	return w, nil
}

func (m *Manager) httpReceiveJob(c *gin.Context) {
	job := job.Job{}
	if err := c.BindJSON(&job); err != nil {
		c.JSON(400, "Failed to parse container")
	}
	m.JobQueue = append(m.JobQueue, job)
	c.JSON(200, "")
}

func (m *Manager) httpExperimentDone(c *gin.Context) {
	log.Print("hello?")
	m.experimentDone = true
	c.JSON(200, "")
}

func (m *Manager) Start() error {
	m.startTime = time.Now().Format("YYYY-MM-DD_HH:MM")

	// Expose API to submit jobs
	if m.APIPort != "" {
		r := gin.Default()
		r.POST("/submit-job", m.httpReceiveJob)
		r.POST("/experiment-done", m.httpExperimentDone)
		go manners.ListenAndServe("localhost:"+m.APIPort, r)
	} else if m.RPCPort != "" {
		rpc.Register(&m)
		rpc.HandleHTTP()
		http.ListenAndServe(m.RPCPort, nil)
	} else {
		return errors.New("could not setup endpoint to receive jobs. Check HTTP or RCP config")
	}

	// main loop
	for {
		done := make(chan bool, 1)
		t0 := time.Now()
		go step(done, t0, m)
		<-done

		if m.stopCondition() {
			break
		} else {
			m.currentTimeStep += 1
		}
	}

	for _, w := range m.workers {
		if err := w.StopMeter(); err != nil {
			log.Println("Worker meter failure", w.Name)
			return err
		}
	}

	manners.Close()
	return nil
}

func (m *Manager) logStats(w *worker.ManagerWorker) error {
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
	stats.MachineID = w.Name

	m.statsLogger.Add(stats)
	return nil
}

func (m *Manager) stopCondition() bool {
	log.Print(m.experimentDone, m.currentNumJobsRunning)
	if m.currentTimeStep >= m.maxTimeStep {
		return true
	} else if m.experimentDone && m.currentNumJobsRunning == 0 {
		return true
	}
	return false
}
