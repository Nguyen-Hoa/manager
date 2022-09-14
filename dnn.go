package manager

import (
	"log"

	"github.com/Nguyen-Hoa/worker"
	tf "github.com/galeone/tensorflow/tensorflow/go"
	tg "github.com/galeone/tfgo"
)

type DNN struct {
	ModelPath string
	model     *tg.Model
}

func (b DNN) Init(modelPath string) error {
	b.model = tg.LoadModel(modelPath, []string{"serve"}, nil)
	return nil
}

func (b DNN) Predict(w *worker.BaseWorker) (interface{}, error) {
	stats := worker.GetStats()
	input_ := []float64{
		stats["freq"],
		stats["user_time"],
		stats["vmem"],
		stats["percent"],
		stats["syscalls"],
		stats["shared"],
		stats["interrupts"],
		stats["sw_interrupts"],
		stats["pids"],
		stats["instructions"],
		stats["missRatio"],
	}
	input, _ := tf.NewTensor(input_)
	res := b.model.Exec(
		[]tf.Output{b.model.Op("StatefulPartitionedCall", 0)},
		map[tf.Output]*tf.Tensor{b.model.Op("serving_default_inputs_input", 0): input},
	)
	log.Print(res)
	return res[0].Value(), nil
}
