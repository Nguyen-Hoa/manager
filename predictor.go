package manager

import "github.com/Nguyen-Hoa/worker"

type BasePredictor struct {
	Name string
	Type string
}

func (b *BasePredictor) predict(w worker.BaseWorker) (float64, error) {
	return 0.0, nil
}
