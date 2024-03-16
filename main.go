package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bpicori/1brc/utils"
)

type Data struct {
	min   float64
	max   float64
	sum   float64
	count int
}

const CHUNK_SIZE = 1 * 1024 * 1024 // MB
const FILE = "./measurements.txt"

var WORKER_COUNT = runtime.NumCPU()
var count atomic.Uint64
var mapMutex sync.Mutex

var CityMap sync.Map

func processWorker(linesRaw *string) {

	lines := strings.Split(*linesRaw, "\n")

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

		v, loaded := CityMap.LoadOrStore(city, &Data{min: temperature, max: temperature, sum: temperature, count: 1})

		if loaded {
			data := v.(*Data)
			data.min = math.Min(data.min, temperature)
			data.max = math.Max(data.max, temperature)
			data.sum += temperature
			data.count++
		}
		// count.Add(1)
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
		fmt.Print("\033[H\033[2J")
		fmt.Println("Count per second: ", utils.HumanizeNumber(rate))
		fmt.Println("Total count: ", utils.HumanizeNumber(float64(currentCount)))
		fmt.Println("Time elapsed: ", utils.HumanizeTime(time.Since(totalTime)))
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
	// cpuFile, err := os.Create("cpu.prof")
	// if err != nil {
	// 	log.Fatal("could not create CPU profile: ", err)
	// }
	// if err := pprof.StartCPUProfile(cpuFile); err != nil {
	// 	log.Fatal("could not start CPU profile: ", err)
	// }
	// defer pprof.StopCPUProfile()

	// parse args

	now := time.Now()

	publishCh := make(chan string)
	wg := sync.WaitGroup{}

	for i := 0; i < WORKER_COUNT; i++ {
		wg.Add(1)
		go func() {
			fmt.Printf("Worker %d started\n", i)
			defer wg.Done()
			for lines := range publishCh {
				processWorker(&lines)
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
	CityMap.Range(func(key, value interface{}) bool {
		city := key.(string)
		data := value.(*Data)
		fmt.Printf("City: %s, Min: %.2f, Max: %.2f, Mean: %.2f\n", city, data.min, data.max, data.sum/float64(data.count))
		return true // continue iteration
	})

	fmt.Println("Time elapsed: ", time.Since(now))

}
