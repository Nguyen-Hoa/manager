package manager

import "github.com/Nguyen-Hoa/worker"

type Predictor interface {
	Init(string) error
	Predict(*worker.ManagerWorker) (float32, error)
}
