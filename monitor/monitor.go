package monitor

import (
    "fmt"
    "math"
    "runtime"
    "strconv"
    "time"
)

var bytesReadCount int64
var previousBytesReadCount int64
var startTime time.Time

// Initialize sets up the monitor with the starting time
func Initialize() {
    startTime = time.Now()
    previousBytesReadCount = 0
}

// UpdateBytesRead adds to the total bytes read counter
func UpdateBytesRead(n int) {
    bytesReadCount += int64(n)
}

// Start begins the monitoring process
func Start(workerCount int) {
    for {
        <-time.After(1 * time.Second)

        // Get current memory stats
        var m runtime.MemStats
        runtime.ReadMemStats(&m)

        fmt.Print("\033[H\033[2J") // Clear the screen
        fmt.Println("Metrics Table")
        fmt.Println("----------------------------------------")
        fmt.Printf("%-25s %s\n", "Metric", "Value")
        fmt.Println("----------------------------------------")
        fmt.Printf("%-25s %d\n", "Number of workers:", workerCount)
        fmt.Printf("%-25s %s\n", "Bytes read:", HumanizeBytes(bytesReadCount))
        fmt.Printf("%-25s %s\n", "Bytes read per second:", HumanizeBytes(bytesReadCount-previousBytesReadCount))
        previousBytesReadCount = bytesReadCount
        fmt.Printf("%-25s %s\n", "Time elapsed:", HumanizeTime(time.Since(startTime)))

        // Memory usage metrics
        fmt.Println("----------------------------------------")
        fmt.Printf("%-25s %s\n", "Heap in use:", HumanizeBytes(int64(m.HeapInuse)))
        fmt.Printf("%-25s %s\n", "Stack in use:", HumanizeBytes(int64(m.StackInuse)))
        fmt.Printf("%-25s %s\n", "Total alloc (cumulative):", HumanizeBytes(int64(m.TotalAlloc)))
        fmt.Printf("%-25s %s\n", "Sys memory:", HumanizeBytes(int64(m.Sys)))
        fmt.Printf("%-25s %d\n", "GC cycles:", m.NumGC)
        fmt.Printf("%-25s %s\n", "GC CPU fraction:", fmt.Sprintf("%.2f%%", m.GCCPUFraction*100))
        fmt.Println("----------------------------------------")
    }
}

// HumanizeNumber formats a number with K, M, B suffixes
func HumanizeNumber(num float64) string {
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

// HumanizeTime formats a duration in a human-readable way
func HumanizeTime(duration time.Duration) string {
    if duration < time.Minute*2 {
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

// HumanizeBytes formats bytes in a human-readable way
func HumanizeBytes(bytes int64) string {
    units := []string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
    if bytes < 1024 {
        return fmt.Sprintf("%d B", bytes)
    }
    exp := int(math.Log(float64(bytes)) / math.Log(1024))
    if exp > len(units)-1 {
        return fmt.Sprintf("%d B", bytes)
    }
    return fmt.Sprintf("%.1f %s", float64(bytes)/math.Pow(1024, float64(exp)), units[exp])
}