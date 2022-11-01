package manager

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	logger "github.com/Nguyen-Hoa/csvlogger"
	job "github.com/Nguyen-Hoa/job"
	predictor "github.com/Nguyen-Hoa/predictor"
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
	ReducedStats bool

	// models
	predictor predictor.Predictor
	scheduler scheduler.Scheduler

	// status
	workers               map[string]*worker.ManagerWorker
	startTime             string
	currentTimeStep       int
	currentNumJobsRunning int
	running               bool
	ExperimentDone        bool
	JobQueue              *job.SharedJobsArray
	APIPort               string
	APIEngine             *gin.Engine
	RPCPort               string

	// loggers
	baseLogPath     string
	statsLogger     logger.Logger
	latencyLogger   logger.Logger
	containerLogger logger.Logger
}

type ManagerConfig struct {
	Logging                bool                  `json:"logging"`
	Verbose                bool                  `json:"verbose"`
	Debug                  bool                  `json:"debug"`
	MaxTimeStep            int                   `json:"maxTimeStep"`
	StepSize               int                   `json:"stepSize"`
	PredictorType          string                `json:"predictorType"`
	ModelPath              string                `json:"modelPath"`
	InferenceServerAddress string                `json:"inferenceServerAddress"`
	SchedulerType          string                `json:"schedulerType"`
	Workers                []worker.WorkerConfig `json:"workers"`
	APIPort                string                `json:"apiPort"`
	RPCPort                string                `json:"rpcPort"`
	BaseLogPath            string                `json:"baseLogPath"`
	ReducedStats           bool                  `json:"reducedStats"`
	ExperimentPath         string                `json:"experimentPath"`
	Jobs                   []job.Job             `json:"jobs"`
}

type Latency struct {
	MachineID string
	Type      string
	Latency   time.Duration
}

type ReducedStatsParams struct {
	Freq       float64 `json:"freq"`
	VMem       float64 `json:"vmem"`
	CPUPercent float64 `json:"cpupercent"`
	Shared     uint64  `json:"shared"`
	Timestamp  string  `json:"timestamp"`
	MachineID  string
}

func (m *Manager) Init(config ManagerConfig) error {
	m.logging = config.Logging
	m.verbose = config.Verbose
	m.debug = config.Debug
	m.maxTimeStep = config.MaxTimeStep
	m.stepSize = time.Duration(config.StepSize) * time.Second
	m.HasPredictor = false

	if config.ExperimentPath != "" {
		m.baseLogPath = filepath.Join(config.BaseLogPath, config.ExperimentPath)
	} else {
		m.baseLogPath = filepath.Join(config.BaseLogPath, time.Now().Format("2006_01_02-15:04:05"))
	}
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
	containerLogger, err := logger.NewLogger(m.baseLogPath, "container")
	if err != nil {
		return err
	}
	m.containerLogger = containerLogger

	m.JobQueue = &job.SharedJobsArray{}
	for _, j := range config.Jobs {
		m.JobQueue.Append(j)
	}

	m.workers = make(map[string]*worker.ManagerWorker)
	for _, w := range config.Workers {
		_worker, err := worker.New(w)
		if err != nil {
			log.Println("Failed to initialize worker ", w.Name, err)
			return err
		}
		m.workers[w.Name] = _worker
		log.Println("Initialized", w.Name, " with power meter: ", _worker.HasPowerMeter)
	}

	m.initPredictor(config)
	m.initScheduler(config)
	m.initAPI(config)

	m.startTime = ""
	m.currentTimeStep = 0
	m.currentNumJobsRunning = 0
	m.running = false
	m.ExperimentDone = false
	m.ReducedStats = config.ReducedStats

	summary, err := json.Marshal(config)
	if err != nil {
		fmt.Println(err)
	}
	err = os.WriteFile(m.baseLogPath+"/summary.out", summary, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return nil
}

func ParseConfig(configPath string) ManagerConfig {
	jsonFile, err := os.Open(configPath)
	if err != nil {
		log.Fatal("Failed to parse configuration file.")
	}
	defer jsonFile.Close()
	byteValue, _ := io.ReadAll(jsonFile)
	var config ManagerConfig
	json.Unmarshal([]byte(byteValue), &config)
	return config
}

func (m *Manager) initPredictor(config ManagerConfig) error {
	if config.PredictorType == "analytical" {
		predictor := predictor.Analytical{}
		m.predictor = &predictor
		m.HasPredictor = true
	} else if config.PredictorType == "dnn" {
		predictor := predictor.DNN{}
		predictor.Init(config.ModelPath)
		m.predictor = &predictor
		m.HasPredictor = true
	} else if config.PredictorType == "server" {
		predictor := predictor.InferenceServer{}
		predictor.Init(config.InferenceServerAddress)
		m.predictor = &predictor
		m.HasPredictor = true
	} else {
		m.predictor = nil
		m.HasPredictor = false
		log.Print("no predictor initialized.")
	}
	return nil
}

func (m *Manager) initScheduler(config ManagerConfig) error {
	if config.SchedulerType == "FIFO" {
		m.scheduler = &scheduler.FIFO{}
		log.Print("initialized scheduler: FIFO")
	} else if config.SchedulerType == "Agent" {
		m.scheduler = &scheduler.Agent{}
		log.Print("initialized scheduler: Round Robin")
	} else if config.SchedulerType == "RoundRobin" {
		if scheduler, err := scheduler.NewRoundRobin(m.workers); err != nil {
			log.Print(err)
			return err
		} else {
			m.scheduler = scheduler
		}
		log.Print("initialized scheduler: RoundRobin")
	} else {
		return errors.New("no scheduler defined")
	}
	return nil
}

func (m *Manager) initAPI(config ManagerConfig) error {
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

	// Expose API to submit jobs
	if m.APIPort != "" {
		r := gin.Default()
		r.POST("/submit-job", m.httpReceiveJob)
		r.POST("/experiment-done", m.httpExperimentDone)
		r.POST("/schedule", m.httpSchedule)
		if config.SchedulerType == "Agent" {
			r.GET("/state", m.httpGetState)
		}
		m.APIEngine = r
		return nil
	} else if m.RPCPort != "" {
		rpc.Register(&m)
		rpc.HandleHTTP()
		http.ListenAndServe(m.RPCPort, nil)
		return nil
	} else {
		return errors.New("could not setup endpoint to receive jobs. Check HTTP or RCP config")
	}
}

func NewManager(configPath string) (Manager, error) {
	m := Manager{}
	config := ParseConfig(configPath)

	if err := m.Init(config); err != nil {
		log.Println("Failed to initialize manager!")
		log.Fatal(err)
		return m, err
	}
	log.Println("Initialized manager")
	return m, nil
}

func NewManagerWithConfig(config ManagerConfig) (Manager, error) {
	m := Manager{}
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
			stats, err := w.Stats(m.ReducedStats)
			tPoll := time.Since(t0)
			if err != nil {
				log.Printf("Error updating stats for %s", w.Name)
			} else {
				w.LatestCPU = float32(stats["cpupercent"].(float64))
				m.latencyLogger.Add(Latency{w.Name, "poll", tPoll})
				m.logStats(w)
				jobsRunning += len(w.RunningJobStats)
			}
		}(w)
	}
	pollWaitGroup.Wait()
	m.currentNumJobsRunning = jobsRunning
	if m.currentNumJobsRunning > 0 {
		m.running = true
	}

	// Inference
	if m.HasPredictor {
		// log.Println("Inference...")
		for _, w := range m.workers {
			t0 := time.Now()
			prediction, err := m.predictor.Predict(w)
			if prediction != 0 {
				w.LatestPredictedPower = prediction
				log.Print(prediction)
			}
			tInference := time.Since(t0)
			m.latencyLogger.Add(Latency{w.Name, "inference", tInference})
			if err != nil {
				log.Print(err)
				log.Printf("failed to predict for %s", w.Name)
			}
		}
	}

	// Assign Job(s)
	if m.JobQueue.Length() > 0 {
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

func (m *Manager) httpReceiveJob(c *gin.Context) {
	job := job.Job{}
	if err := c.BindJSON(&job); err != nil {
		c.JSON(400, "Failed to parse container")
	}
	m.JobQueue.Append(job)
	c.JSON(200, "")
}

func (m *Manager) httpExperimentDone(c *gin.Context) {
	m.ExperimentDone = true
	c.JSON(200, "")
}

func (m *Manager) httpGetState(c *gin.Context) {
	state := scheduler.Agent.State(scheduler.Agent{}, m.workers)
	c.JSON(200, state)
}

func (m *Manager) httpSchedule(c *gin.Context) {
	type ScheduleRequest struct {
		MachineID string  `json:"target"`
		Job       job.Job `json:"job"`
	}
	body := ScheduleRequest{}
	if err := c.BindJSON(&body); err != nil {
		c.JSON(400, "Failed to parse container")
	}
	target, job := body.MachineID, body.Job
	if err := m.workers[target].StartJob(job.Image, job.Cmd, job.Duration); err != nil {
		c.JSON(500, err)
	}
	c.JSON(200, nil)
}

func (m *Manager) Start() error {
	tExp := time.Now()
	m.startTime = tExp.Format("YYYY-MM-DD_HH:MM")
	go manners.ListenAndServe("localhost:"+m.APIPort, m.APIEngine)
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
		if w.HasPowerMeter {
			if path, err := w.StopMeter(); err != nil {
				log.Println("Worker meter failure", w.Name)
				return err
			} else {
				src := fmt.Sprintf("\n%s:~/jerry/server/%s\n", w.Address[7:len(w.Address)-5], path)
				f, err := os.OpenFile(m.baseLogPath+"/summary.out",
					os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					log.Println(err)
				}
				defer f.Close()
				if _, err := f.WriteString(src); err != nil {
					log.Println(err)
				}
			}
		}
	}

	manners.Close()
	stopTime := time.Since(tExp)
	log.Print(stopTime)
	return nil
}

func (m *Manager) logStats(w *worker.ManagerWorker) error {
	stats_ := w.GetStats()
	marsh_stats, err := json.Marshal(stats_)
	if err != nil {
		log.Print(err)
		return err
	}

	if m.ReducedStats {
		stats := ReducedStatsParams{}
		if err := json.Unmarshal(marsh_stats, &stats); err != nil {
			log.Print(err)
			return err
		}
		stats.MachineID = w.Name
		m.statsLogger.Add(stats)
	} else {
		stats := profile.DNN_params{}
		if err := json.Unmarshal(marsh_stats, &stats); err != nil {
			log.Print(err)
			return err
		}
		stats.MachineID = w.Name
		m.statsLogger.Add(stats)
	}

	// container logging
	time := stats_["timestamp"].(string)
	for key := range w.RunningJobStats {
		baseStats := w.RunningJobStats[key].(map[string]interface{})
		memStats := baseStats["memory_stats"].(map[string]interface{})
		cpuStats := baseStats["cpu_stats"].(map[string]interface{})
		cpuUsageStats := cpuStats["cpu_usage"].(map[string]interface{})
		memusage := memStats["usage"]
		cpuusage := cpuUsageStats["total_usage"]
		if memusage != nil && cpuusage != nil {
			type ContainerStats struct {
				MachineID string
				Timestamp string
				MemUsage  float64
				CpuUsage  float64
			}
			ctrStats := ContainerStats{
				w.Name,
				time,
				memStats["usage"].(float64),
				cpuUsageStats["total_usage"].(float64),
			}
			m.containerLogger.Add(ctrStats)
		}
	}

	return nil
}

func (m *Manager) state() {
	type MachineStats struct {
		MMU float64
		MCU float64
	}
	type ContainerStats struct {
		CMU float64
		CCU float64
	}
	type WorkerStats struct {
		MachineID string
		MachineStats
		ContainerStats
	}

	state := make([]WorkerStats, 4)
	for w := range m.workers {
		machine_stats := m.workers[w].GetStats()
		m_stats := MachineStats{machine_stats["VMem"].(float64), machine_stats["Freq"].(float64)}

		total_mem_usage := 0.0
		total_cpu_usage := 0.0
		for c := range m.workers[w].RunningJobStats {
			baseStats := m.workers[w].RunningJobStats[c].(map[string]interface{})
			memStats := baseStats["memory_stats"].(map[string]interface{})
			cpuStats := baseStats["cpu_stats"].(map[string]interface{})
			cpuUsageStats := cpuStats["cpu_usage"].(map[string]interface{})
			memusage := memStats["usage"]
			cpuusage := cpuUsageStats["total_usage"]
			if memusage != nil && cpuusage != nil {
				total_mem_usage += memStats["usage"].(float64)
				total_cpu_usage += cpuUsageStats["total_usage"].(float64)
			}
		}
		c_stats := ContainerStats{total_mem_usage, total_cpu_usage}

		stat := WorkerStats{
			w,
			m_stats,
			c_stats,
		}
		state = append(state, stat)
	}
}

func (m *Manager) stopCondition() bool {
	if m.currentTimeStep >= m.maxTimeStep {
		return true
	}

	if m.running && m.currentNumJobsRunning == 0 {
		return true
	}
	return false
}
