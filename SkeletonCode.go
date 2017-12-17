package main

import (
    "fmt"
    "os"
    "strconv"
    "math"
	"encoding/csv"
)

type CensusGroup struct {
	population int
	latitude, longitude float64
}

func ParseCensusData(fname string) ([]CensusGroup, error) {
	file, err := os.Open(fname)
    if err != nil {
		return nil, err
    }
    defer file.Close()

	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		return nil, err
	}
	censusData := make([]CensusGroup, 0, len(records))

    for _, rec := range records {
        if len(rec) == 7 {
            population, err1 := strconv.Atoi(rec[4])
            latitude, err2 := strconv.ParseFloat(rec[5], 64)
            longitude, err3 := strconv.ParseFloat(rec[6], 64)
            if err1 == nil && err2 == nil && err3 == nil {
                latpi := latitude * math.Pi / 180
                latitude = math.Log(math.Tan(latpi) + 1 / math.Cos(latpi))
                censusData = append(censusData, CensusGroup{population, latitude, longitude})
            }
        }
    }

	return censusData, nil
}

func main () {
	if len(os.Args) < 4 {
		fmt.Printf("Usage:\nArg 1: file name for input data\nArg 2: number of x-dim buckets\nArg 3: number of y-dim buckets\nArg 4: -v1, -v2, -v3, -v4, -v5, or -v6\n")
		return
	}
	fname, ver := os.Args[1], os.Args[4]
    xdim, err := strconv.Atoi(os.Args[2])
	if err != nil {
		fmt.Println(err)
		return
	}
    ydim, err := strconv.Atoi(os.Args[3])
	if err != nil {
		fmt.Println(err)
		return
	}
	censusData, err := ParseCensusData(fname)
	if err != nil {
		fmt.Println(err)
		return
	}

    switch ver {
    case "-v1":
        // YOUR SETUP CODE FOR PART 1
    case "-v2":
        // YOUR SETUP CODE FOR PART 2
    case "-v3":
        // YOUR SETUP CODE FOR PART 3
    case "-v4":
        // YOUR SETUP CODE FOR PART 4
    case "-v5":
        // YOUR SETUP CODE FOR PART 5
    case "-v6":
        // YOUR SETUP CODE FOR PART 6
    default:
        fmt.Println("Invalid version argument")
        return
    }

    for {
        var west, south, east, north int
        n, err := fmt.Scanln(&west, &south, &east, &north)
        if n != 4 || err != nil || west<1 || west>xdim || south<1 || south>ydim || east<west || east>xdim || north<south || north>ydim {
            break
        }

        var population int
        var percentage float64
        switch ver {
        case "-v1":
            // YOUR QUERY CODE FOR PART 1
        case "-v2":
            // YOUR QUERY CODE FOR PART 2
        case "-v3":
            // YOUR QUERY CODE FOR PART 3
        case "-v4":
            // YOUR QUERY CODE FOR PART 4
        case "-v5":
            // YOUR QUERY CODE FOR PART 5
        case "-v6":
            // YOUR QUERY CODE FOR PART 6
        }

        fmt.Printf("%v %.2f%%\n", population, percentage)
    }
}
