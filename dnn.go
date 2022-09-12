package manager

import (
	"github.com/Nguyen-Hoa/worker"
	tg "github.com/galeone/tfgo"
)

type DNN struct {
	ModelPath string
	model     *tg.Model
}

func (b DNN) Init(modelPath string) error {
	b.model = tg.LoadModel(modelPath, []string{"server"}, nil)
	return nil
}

func (b DNN) Predict(w worker.BaseWorker) (float64, error) {
	return 0.0, nil
}
