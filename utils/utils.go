package utils

import (
	"fmt"
	"math"
	"strconv"
	"time"
)

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

func HumanizeTime(duration time.Duration) string {
	if duration < time.Minute * 2 {
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
