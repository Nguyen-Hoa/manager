package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"

	"github.com/Nguyen-Hoa/worker"
)

type InferenceServer struct {
	address string
}

func (s *InferenceServer) Init(address string) error {
	s.address = address

	resp, err := http.Get(address + "/api/status")
	if err != nil {
		return err
	}

	if resp.StatusCode == 200 {
		log.Print("inference server connection established")
		return nil
	}

	return errors.New("unknown status code")
}

func (s *InferenceServer) Predict(w *worker.BaseWorker) (interface{}, error) {
	stats := w.GetStats()
	json_data, err := json.Marshal(stats)
	if err != nil {
		return nil, err
	}
	resp, err := http.Post(s.address+"/api/predict", "application/json", bytes.NewBuffer(json_data))
	if err != nil {
		return nil, err
	} else if resp.StatusCode != 200 {
		return nil, errors.New("inference server failed to respond")
	}
	defer resp.Body.Close()
	var res map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&res)
	prediction, _ := strconv.ParseFloat(res["prediction"].(string), 32)
	return prediction, nil
}
