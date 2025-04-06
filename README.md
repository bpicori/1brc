# One Billion Row Challenge

This is my implementation of the [One Billion Row Challenge](https://www.morling.dev/blog/one-billion-row-challenge/) - a performance challenge to process a large dataset of weather measurements as quickly as possible.

## How to Run

1. Clone the challenge repository: `git clone git@github.com:gunnarmorling/1brc.git`

2. Make sure to have Java installed and then run:
  ```bash
      ./mvnw clean verify
      ./create_measurements.sh 1000000000
  ```

3. Run the program: `go run main.go`

## Optimizations

- Map-reduce approach with parallel workers
- Efficient chunk-based file reading - instead of reading line by line, we read in chunks of 4MB
- Process the data as bytes instead of strings to avoid string allocations
- Custom line splitting instead of using string.Split
- Custom temperature parsing function instead of standard string conversion
- Optimized city name deduplication using integer indices instead of string keys

## Architecture

```
                                  ┌─────────────┐
                                  │   Monitor   │
                                  │  Goroutine  │
                                  └─────────────┘
                                        │
                                        ▼
┌─────────────┐    ┌───────────┐    ┌─────────┐    ┌───────────────┐
│  File Reader│───▶│ publishCh │───▶│ Workers │───▶│ Results Channel│
│  Goroutine  │    └───────────┘    │(Map)    │    └───────────────┘
└─────────────┘                     └─────────┘            │
                                                           ▼
                                                   ┌───────────────┐
                                                   │ Reduce Phase  │
                                                   │ (Main Thread) │
                                                   └───────────────┘
                                                           │
                                                           ▼
                                                   ┌───────────────┐
                                                   │ Save Results  │
                                                   └───────────────┘
```
