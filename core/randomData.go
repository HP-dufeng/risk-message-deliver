package core

import (
	"math/rand"
	"time"
)

func randomIndex(length int) int {
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)

	return r.Intn(length - 1)
}

func randomFloat(length int) float64 {
	source := rand.NewSource(time.Now().UnixNano())
	r := rand.New(source)

	return float64(r.Intn(length-1)) + r.Float64()
}

func randomActionFlags() uint32 {
	actionFlags := []uint32{1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2, 1, 2}

	return actionFlags[randomIndex(len(actionFlags))]

}
