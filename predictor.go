package manager

import "github.com/Nguyen-Hoa/worker"

type BasePredictor interface {
	Init(string) error
	Predict(worker.BaseWorker) (float64, error)
}
