package manager

import (
	"log"
	"testing"
)

func TestInit(t *testing.T) {
	m, err := NewManager("../controller/config.json")
	if err != nil {
		log.Print(err)
	}

	log.Print(m)
}
