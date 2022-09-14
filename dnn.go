package manager

import (
	"log"

	"github.com/Nguyen-Hoa/worker"
	tf "github.com/galeone/tensorflow/tensorflow/go"
)

type DNN struct {
	ModelPath string
	model     *tf.SavedModel
}

func (b *DNN) Init(modelPath string) error {
	model, err := tf.LoadSavedModel(modelPath, []string{"serve"}, nil)
	if err != nil {
		return err
	}
	b.model = model	
	// for _, op := range b.model.Graph.Operations() {
	// 	log.Print(op.Name())
	// }

	return nil
}

func (b *DNN) Predict(w *worker.BaseWorker) (interface{}, error) {
	stats := w.GetStats()
	input_ := [][]float64{{
		float64(stats["freq"].(float64)),
		float64(stats["user_time"].(float64)),
		float64(stats["vmem"].(float64)),
		float64(stats["percent"].(float64)),
		float64(stats["syscalls"].(float64)),
		float64(stats["shared"].(float64)),
		float64(stats["interrupts"].(float64)),
		float64(stats["sw_interrupts"].(float64)),
		float64(stats["pids"].(float64)),
		float64(stats["instructions"].(float64)),
		float64(stats["missRatio"].(float64)),
	}}

	input, _ := tf.NewTensor(input_)
	log.Print("starting infer.")
	res, err  := b.model.Session.Run(
		map[tf.Output]*tf.Tensor{
			b.model.Graph.Operation("serving_default_args_0").Output(0): input,
		},
		[]tf.Output{
			b.model.Graph.Operation("StatefulPartitionedCall").Output(0),
		},
		nil,
	)
	if err != nil {
		log.Print("erroneous", err)
		return nil, err
	}
	log.Print(res[0].Value().([][]float32)[0][0])
	return res[0].Value(), nil
}
