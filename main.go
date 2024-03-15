package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type Data struct {
	min  float64
	max  float64
	mean float64
}

var count atomic.Uint64
var mapMutex sync.Mutex

var CityMap = make(map[string]Data)

func hashCity(city string, mod int) int {
	hash := 0

	for i := 0; i < len(city); i++ {
		hash = (hash*31 + int(city[i])) % mod
	}
	return hash

}

func processWorker(line string) {
	// mapMutex.Lock()
	// defer mapMutex.Unlock()

	res := strings.Split(line, ";")
	city := res[0]
	temperature, err := strconv.ParseFloat(res[1], 64)
	if err != nil {
		panic(err)
	}

	if _, ok := CityMap[city]; !ok {
		// CityMap[city] = Data{min: temperature, max: temperature, mean: temperature}
	} else {
		// update the data
		data := CityMap[city]
		if temperature < data.min {
			data.min = temperature
		}
		if temperature > data.max {
			data.max = temperature
		}
		data.mean = (data.mean + temperature) / 2
		// CityMap[city] = data
	}
	count.Add(1)
}

func humanizeNumber(num float64) string {
	units := []string{"", "K", "M", "B", "T", "P", "E"}
	if num < 1000 {
		return fmt.Sprintf("%.0f", num)
	}
	exp := int(math.Log(num) / math.Log(1000))
	if exp > len(units)-1 {
		return strconv.FormatFloat(num, 'f', -1, 64)
	}
	return fmt.Sprintf("%.1f%s", num/math.Pow(1000, float64(exp)), units[exp])
}

func humanizeTime(duration time.Duration) string {
	if duration < time.Minute {
		// Less than a minute
		return fmt.Sprintf("%d seconds", int(duration.Seconds()))
	} else if duration < time.Hour {
		// Less than an hour
		return fmt.Sprintf("%d minutes", int(duration.Minutes()))
	} else if duration < time.Hour*24 {
		// Less than a day
		hours := duration / time.Hour
		duration -= hours * time.Hour
		minutes := duration / time.Minute
		return fmt.Sprintf("%d hours %d minutes", hours, minutes)
	} else {
		// Days or more
		days := duration / (time.Hour * 24)
		duration -= days * (time.Hour * 24)
		hours := duration / time.Hour
		return fmt.Sprintf("%d days %d hours", days, hours)
	}
}

func main() {
	file, err := os.Open("./measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	previousCount := count.Load()
	totalTime := time.Now()

	// create a go routine that prints the count every 5 seconds
	go func() {
		for {
			<-time.After(1 * time.Second)
			// calculate the count per second
			currentCount := count.Load()
			rate := float64(currentCount - previousCount)
			fmt.Println("Count per second: ", humanizeNumber(rate))
			previousCount = currentCount

			fmt.Println("Total count: ", humanizeNumber(float64(currentCount)))
			fmt.Println("Time elapsed: ", humanizeTime(time.Since(totalTime)))

		}
	}()

	publishCh := make(chan string)

	// create 10 workers
	for i := 0; i < 10; i++ {
		go func() {
			fmt.Printf("Worker %d started\n", i)
			for {
				item := <-publishCh
				processWorker(item)
			}
		}()
	}

	for scanner.Scan() {
		line := scanner.Text()
		// publish the item to the workers
		publishCh <- line
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	// print the result
	for city, data := range CityMap {
		fmt.Printf("City: %s, Min: %.2f, Max: %.2f, Mean: %.2f\n", city, data.min, data.max, data.mean)
	}

}
