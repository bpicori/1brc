package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"
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
var mapMutex sync.Mutex
var bytesReadCount int64

var CityMap sync.Map

func parseTemp(tempBytes string) float64 {
	negative := false
	index := 0
	if tempBytes[index] == '-' {
		index++
		negative = true
	}
	temp := float64(tempBytes[index] - '0')
	index++
	if tempBytes[index] != '.' {
		temp = temp*10 + float64(tempBytes[index]-'0')
		index++
	}
	index++ // skip '.'
	temp += float64(tempBytes[index]-'0') / 10
	if negative {
		temp = -temp
	}

	return temp
}

func processWorker(linesRaw *string) {

	lines := strings.Split(*linesRaw, "\n")

	for _, line := range lines {
		if line == "" {
			continue
		}
		res := strings.Split(line, ";")
		city := res[0]
		temperature := parseTemp(res[1])

		v, loaded := CityMap.LoadOrStore(city, &Data{min: temperature, max: temperature, sum: temperature, count: 1})

		if loaded {
			data := v.(*Data)
			data.min = math.Min(data.min, temperature)
			data.max = math.Max(data.max, temperature)
			data.sum += temperature
			data.count++
		}
	}
}

func monitor() {
	totalTime := time.Now()
	previousBytesReadCount := int64(0)
	for {
		<-time.After(1 * time.Second)
		fmt.Print("\033[H\033[2J") // Clear the screen
		fmt.Println("Metrics Table")
		fmt.Println("----------------------------------------")
		fmt.Printf("%-25s %s\n", "Metric", "Value")
		fmt.Println("----------------------------------------")
		fmt.Printf("%-25s %s\n", "Bytes read:", utils.HumanizeBytes(bytesReadCount))
		fmt.Printf("%-25s %s\n", "Bytes read per second:", utils.HumanizeBytes(bytesReadCount-previousBytesReadCount))
		previousBytesReadCount = bytesReadCount
		fmt.Printf("%-25s %s\n", "Time elapsed:", utils.HumanizeTime(time.Since(totalTime)))
		fmt.Println("----------------------------------------")

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
		bytesReadCount += int64(n)

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

	// memFile, err := os.Create("mem.prof")
	// if err != nil {
	// 	log.Fatal("could not create memory profile: ", err)
	// }
	// runtime.GC() // get up-to-date statistics
	// if err := pprof.WriteHeapProfile(memFile); err != nil {
	// 	log.Fatal("could not write memory profile: ", err)
	// }
	// memFile.Close()

	// parse args

	now := time.Now()

	go monitor()

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
