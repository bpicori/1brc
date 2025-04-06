package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"sync"
	"time"

	"github.com/bpicori/1brc/monitor"
)

type Data struct {
	min   float64
	max   float64
	sum   float64
	count int
}

type WorkerResult struct {
	cityData map[string]*Data
}

const CHUNK_SIZE = 4 * 1024 * 1024 // MB
const FILE = "./measurements.txt"

var WORKER_COUNT = runtime.NumCPU()
var bytesReadCount int64
var CityMap map[string]*Data

func parseTemp(tempBytes []byte) float64 {
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
func getCityAndTemp(line []byte) ([]byte, []byte) {
	for i := 0; i < len(line); i++ {
		if line[i] == ';' {
			return line[:i], line[i+1:]
		}
	}
	return line, nil
}

func splitByNewline(data []byte, processFunc func([]byte)) {
	start := 0
	for i := 0; i < len(data); i++ {
		if data[i] == '\n' {
			processFunc(data[start:i])
			start = i + 1
		}
	}

	// Handle the last line if there's no final newline
	if start < len(data) {
		processFunc(data[start:])
	}
}

func mapPhase(linesRaw *[]byte) WorkerResult {
	// Pre-allocate with expected capacity
	localCityData := make(map[string]*Data, 1000)

	// Create a string-to-int map for city name deduplication
	cityIndices := make(map[string]int, 1000)
	cityNames := make([]string, 0, 1000)
	cityData := make([]*Data, 0, 1000)
	nextIndex := 0

	splitByNewline(*linesRaw, func(line []byte) {
		if line == nil {
			return
		}

		city, tempStr := getCityAndTemp(line)
		temperature := parseTemp(tempStr)

		// Use the integer index instead of string key for map lookups
		idx, exists := cityIndices[string(city)]
		if !exists {
			// First time seeing this city
			idx = nextIndex
			cityIndices[string(city)] = idx
			cityNames = append(cityNames, string(city))
			data := &Data{
				min:   temperature,
				max:   temperature,
				sum:   temperature,
				count: 1,
			}
			cityData = append(cityData, data)
			nextIndex++
		} else {
			// City exists, update its data
			data := cityData[idx]
			if temperature < data.min {
				data.min = temperature
			}
			if temperature > data.max {
				data.max = temperature
			}
			data.sum += temperature
			data.count++
		}
	})

	// Convert back to the expected map format for the result
	for i, name := range cityNames {
		localCityData[name] = cityData[i]
	}

	return WorkerResult{cityData: localCityData}
}

func reducePhase(workerResults []WorkerResult) {
	for _, result := range workerResults {
		for city, data := range result.cityData {
			globalData, exists := CityMap[city]
			if !exists {
				CityMap[city] = data
			} else {
				globalData.min = math.Min(globalData.min, data.min)
				globalData.max = math.Max(globalData.max, data.max)
				globalData.sum += data.sum
				globalData.count += data.count
			}
		}
	}
}

func readFile(publishCh chan []byte) {
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
		monitor.UpdateBytesRead(n) 

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
				publishCh <- newBuff
				break
			}
			restBuff = append(restBuff, newBuff...)
			continue
		}

		publishCh <- newBuff[:lastNewline+1] // last line
		restBuff = append(restBuff, newBuff[lastNewline+1:]...)

		if err != nil {
			if err != io.EOF {
				panic(err)
			}
			break
		}
	}
}

func saveResultsToFile(cityMap *map[string]*Data) error {
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

	for city, data := range *cityMap {
		results = append(results, CityData{
			City: city,
			Min:  data.min,
			Max:  data.max,
			Mean: data.sum / float64(data.count),
		})
	}

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
	// Add CPU profiling
	cpuprofile := flag.String("cpuprofile", "", "write cpu profile to file")
	flag.Parse()

	if *cpuprofile != "" {
		f, err := os.Create(*cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close()
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	// Initialize CityMap
	CityMap = make(map[string]*Data)

	totalStart := time.Now()
	
	// Initialize and start monitor
	monitor.Initialize()
	go monitor.Start(WORKER_COUNT)

	publishCh := make(chan []byte)
	wg := sync.WaitGroup{}
	resultCh := make(chan WorkerResult)

	// Start workers
	mapStart := time.Now()
	for i := 0; i < WORKER_COUNT; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for lines := range publishCh {
				result := mapPhase(&lines)
				resultCh <- result
			}
		}()
	}

	// Start file reader
	readStart := time.Now()
	wg.Add(1)
	go func() {
		defer wg.Done()
		readFile(publishCh)
		readDuration := time.Since(readStart)
		fmt.Printf("File reading completed in: %s\n", readDuration)
		close(publishCh)
	}()

	// Collect results
	var results []WorkerResult
	go func() {
		for result := range resultCh {
			results = append(results, result)
		}
	}()

	wg.Wait()
	mapDuration := time.Since(mapStart)
	fmt.Printf("Map phase completed in: %s\n", mapDuration)
	close(resultCh)

	// Reduce phase
	reducePhase(results)

	// Save results
	saveStart := time.Now()
	err := saveResultsToFile(&CityMap)
	if err != nil {
		fmt.Println("Error saving results to file: ", err)
	}
	saveDuration := time.Since(saveStart)
	fmt.Printf("Results saved in: %s\n", saveDuration)

	// Print total time
	totalDuration := time.Since(totalStart)
	fmt.Println("Total execution time: ", totalDuration)

	// Print performance summary
	fmt.Println("\nPerformance Summary:")
	fmt.Println("----------------------------------------")
	fmt.Printf("%-25s %s\n", "File Reading:", time.Since(readStart))
	fmt.Printf("%-25s %s\n", "Map Phase:", mapDuration)
	fmt.Printf("%-25s %s\n", "Reduce Phase:", time.Since(mapStart)-mapDuration)
	fmt.Printf("%-25s %s\n", "Saving Results:", saveDuration)
	fmt.Printf("%-25s %s\n", "Total Time:", totalDuration)
	fmt.Println("----------------------------------------")
}
