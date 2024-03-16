package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"sort"
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
		fmt.Printf("%-25s %d\n", "Number of workers:", WORKER_COUNT)
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

func saveResultsToFile(cityMap *sync.Map) error {
	type CityData struct {
		City string
		Min  float64
		Max  float64
		Mean float64
	}
	now := time.Now()
	filename := fmt.Sprintf("result-%s.txt", now.Format("20060102150405"))
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	var results []CityData

	cityMap.Range(func(key, value interface{}) bool {
		city := key.(string)
		data := value.(*Data)
		results = append(results, CityData{
			City: city,
			Min:  data.min,
			Max:  data.max,
			Mean: data.sum / float64(data.count),
		})
		return true // continue iteration
	})

	// Sorting the results
	sort.Slice(results, func(i, j int) bool {
		return results[i].City < results[j].City
	})

	// Formatting and writing to the file
	file.WriteString("{")
	for _, data := range results {
		line := fmt.Sprintf("%s=%.1f/%.1f/%.1f, ", data.City, data.Min, data.Mean, data.Max)
		_, err = file.WriteString(line)
		if err != nil {
			return err
		}
	}
	file.WriteString("}")

	return nil
}

func main() {
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

	err := saveResultsToFile(&CityMap)
	if err != nil {
		fmt.Println("Error saving results to file: ", err)
	}

	fmt.Println("Time elapsed: ", time.Since(now))

}
