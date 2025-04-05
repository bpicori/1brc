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
	"runtime/debug"
	"runtime/pprof"
	"sort"
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

type WorkerResult struct {
	cityData map[string]*Data
}

const CHUNK_SIZE = 4 * 1024 * 1024 // MB
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
func getCityAndTemp(line string) (string, string) {
	for i := 0; i < len(line); i++ {
		if line[i] == ';' {
			return line[:i], line[i+1:]
		}
	}
	return line, ""
}

func splitByNewline(data string, processFunc func(string)) {
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

func mapPhase(linesRaw *string) WorkerResult {
	localCityData := make(map[string]*Data, 1000)

	splitByNewline(*linesRaw, func(line string) {
		if line == "" {
			return
		}

		city, tempStr := getCityAndTemp(line)
		temperature := parseTemp(tempStr)

		if data, exists := localCityData[city]; exists {
			if temperature < data.min {
				data.min = temperature
			}
			if temperature > data.max {
				data.max = temperature
			}
			data.sum += temperature
			data.count++
		} else {
			localCityData[city] = &Data{
				min:   temperature,
				max:   temperature,
				sum:   temperature,
				count: 1,
			}
		}
	})

	return WorkerResult{cityData: localCityData}
}

func reducePhase(results []WorkerResult) {
	// For each worker result
	for _, result := range results {
		// For each city in the worker's local map
		for city, data := range result.cityData {
			v, loaded := CityMap.LoadOrStore(city, data)
			if loaded {
				// City already exists in global map, merge the data
				globalData := v.(*Data)
				mapMutex.Lock()
				globalData.min = math.Min(globalData.min, data.min)
				globalData.max = math.Max(globalData.max, data.max)
				globalData.sum += data.sum
				globalData.count += data.count
				mapMutex.Unlock()
			}
			// If not loaded, the worker's data was stored directly
		}
	}
}

func monitor() {
	totalTime := time.Now()
	previousBytesReadCount := int64(0)
	var m runtime.MemStats

	for {
		<-time.After(1 * time.Second)

		// Get current memory stats
		runtime.ReadMemStats(&m)

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

		// Memory usage metrics
		fmt.Println("----------------------------------------")
		fmt.Printf("%-25s %s\n", "Heap in use:", utils.HumanizeBytes(int64(m.HeapInuse)))
		fmt.Printf("%-25s %s\n", "Stack in use:", utils.HumanizeBytes(int64(m.StackInuse)))
		fmt.Printf("%-25s %s\n", "Total alloc (cumulative):", utils.HumanizeBytes(int64(m.TotalAlloc)))
		fmt.Printf("%-25s %s\n", "Sys memory:", utils.HumanizeBytes(int64(m.Sys)))
		fmt.Printf("%-25s %d\n", "GC cycles:", m.NumGC)
		fmt.Printf("%-25s %s\n", "GC CPU fraction:", fmt.Sprintf("%.2f%%", m.GCCPUFraction*100))
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
	// Configure GC before any other operations
	// Set a higher GOGC value to reduce GC frequency
	debug.SetGCPercent(500) // Default is 100, higher means less frequent GC

	// Increase memory limit to avoid GC pressure
	// Using 90% of available system memory (adjust as needed)
	debug.SetMemoryLimit(12 * 1024 * 1024 * 1024) // Example: ~9GB

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

	totalStart := time.Now()

	go monitor()

	publishCh := make(chan string)
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
