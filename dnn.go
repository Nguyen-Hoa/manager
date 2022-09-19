package manager

import (
	"log"

	"github.com/Nguyen-Hoa/worker"
	tf "github.com/galeone/tensorflow/tensorflow/go"
)

type DNN struct {
	ModelPath string
	model     *tf.SavedModel
	transform map[string][]float64
}

func (b *DNN) Init(modelPath string) error {
	model, err := tf.LoadSavedModel(modelPath, []string{"serve"}, nil)
	if err != nil {
		return err
	}
	b.model = model

	transform := make(map[string]([]float64))
	transform["min_"] = []float64{
		-5.87887045e-01,  0.00000000e+00, -1.01010101e-02,  0.00000000e+00,
       -2.07117317e-03, -2.12659125e-03, -1.46082285e-04, -2.07117317e-03,
       -8.39895013e-01, -2.19216965e-05, -2.44619754e-03
	}
	transform["scale_"] = []float64{
		5.87884595e-04, 3.12647040e-02, 1.01010101e-02, 1.00000000e-02,
		1.92012328e-05, 3.60546748e-09, 1.61617208e-06, 1.92012328e-05,
		2.62467192e-03, 9.36211698e-12, 1.00495008e+00
	}
	b.transform = transform
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
	input = fit(input)

	log.Print("starting infer.")
	res, err := b.model.Session.Run(
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

func (b *DNN) fit(input *tf.Tensor) (*tf.Tensor){
	res := input.MatMul(b.transform["scale_"]).Add(b.transform["min_"])
	return res
}