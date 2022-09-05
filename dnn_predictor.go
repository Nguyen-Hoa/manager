package manager

import "github.com/Nguyen-Hoa/worker"

type DNNModel struct {
	BasePredictor
}

func (b *DNNModel) predict(w worker.BaseWorker) (float64, error) {
	return 0.0, nil
}
