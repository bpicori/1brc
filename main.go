package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bpicori/1brc/utils"
)

type Data struct {
	min  float64
	max  float64
	mean float64
}

const CHUNK_SIZE = 16 * 1024 * 1024 // 1MB
const WORKER_COUNT = 8
const FILE = "./measurements.txt"

var count atomic.Uint64
var mapMutex sync.Mutex

var CityMap = make(map[string]Data)

func processWorker(linesRaw string) {
	mapMutex.Lock()
	defer mapMutex.Unlock()

	lines := strings.Split(linesRaw, "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}
		res := strings.Split(line, ";")
		city := res[0]
		temperature, err := strconv.ParseFloat(res[1], 64)
		if err != nil {
			panic(err)
		}

		if _, ok := CityMap[city]; !ok {
			CityMap[city] = Data{min: temperature, max: temperature, mean: temperature}
		} else {
			data := CityMap[city]
			if temperature < data.min {
				data.min = temperature
			}
			if temperature > data.max {
				data.max = temperature
			}
			data.mean = (data.mean + temperature) / 2
			CityMap[city] = data
		}
		count.Add(1)
	}
}

func monitor() {
	previousCount := count.Load()
	totalTime := time.Now()
	for {
		<-time.After(1 * time.Second)
		// calculate the count per second
		currentCount := count.Load()
		rate := float64(currentCount - previousCount)
		previousCount = currentCount
		// fmt.Print("\033[H\033[2J")
		fmt.Println("Count per second: ", utils.HumanizeNumber(rate))
		fmt.Println("Total count: ", utils.HumanizeNumber(float64(currentCount)))
		fmt.Println("Time elapsed: ", utils.HumanizeTime(time.Since(totalTime)))
		fmt.Println("City count: ", len(CityMap))

	}
}

func readFile(publishCh chan string) {
	file, err := os.Open(FILE)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	tempBuff := make([]byte, CHUNK_SIZE)
	restBuff := make([]byte, 0)

	for {
		n, err := reader.Read(tempBuff)

		newBuff := append(restBuff, tempBuff[:n]...)
		restBuff = make([]byte, 0)
		tempBuff = make([]byte, CHUNK_SIZE)

		// find the last newline character
		lastNewline := -1
		for i := len(newBuff) - 1; i >= 0; i-- {
			if newBuff[i] == '\n' {
				lastNewline = i
				break
			}
		}

		// if there is no newline character, append newBuff to restBuff
		if lastNewline == -1 {
			if err == io.EOF {
				publishCh <- string(newBuff)
				break
			}
			restBuff = append(restBuff, newBuff...)
			continue
		}

		lines := string(newBuff[:lastNewline+1])
		publishCh <- lines
		restBuff = append(restBuff, newBuff[lastNewline+1:]...)

		if err != nil {
			if err != io.EOF {
				panic(err)
			}
			break
		}
	}

}

func main() {
	go monitor()

	publishCh := make(chan string)
	wg := sync.WaitGroup{}

	for i := 0; i < WORKER_COUNT; i++ {
		wg.Add(1)
		go func() {
			fmt.Printf("Worker %d started\n", i)
			defer wg.Done()
			for lines := range publishCh {
				processWorker(lines)
			}
		}()
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		readFile(publishCh)
		close(publishCh)
	}()

	wg.Wait()

	// print the result
	for city, data := range CityMap {
		fmt.Printf("City: %s, Min: %.2f, Max: %.2f, Mean: %.2f\n", city, data.min, data.max, data.mean)
	}

}
