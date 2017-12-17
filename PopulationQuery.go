package main

import (
    "fmt"
    "os"
    "strconv"
    "math"
	"encoding/csv"
	// "net"
	// "bufio"
	// "strings"
)

type CensusGroup struct {
	population int
	latitude, longitude float64
}
type minmax struct {
    minimumLatitude, minimumLongitude, maximumLatitude, maximumLongitude float64;
}
type output struct {
    population int
    percentage float64
}
type pair struct {
    gridx, gridy int;
}
type direction struct{
	west, south, east, north int;
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
func findminmax(censusData []CensusGroup) minmax{
    var minimumLatitude  float64 = 9999;
        var minimumLongitude float64  = 9999;
        var maximumLatitude  float64 = -9999;
        var maximumLongitude float64  = -9999;
        for _,coords := range censusData{
            if coords.latitude < minimumLatitude {
                minimumLatitude = coords.latitude;
            }
            if coords.longitude < minimumLongitude {
                minimumLongitude = coords.longitude;
            }
            if coords.latitude > maximumLatitude {
                maximumLatitude = coords.latitude;
            }
            if coords.longitude > maximumLongitude {
                maximumLongitude = coords.longitude;
            }
        }
        return minmax{minimumLatitude, minimumLongitude, maximumLatitude, maximumLongitude};
}
func findminmaxparallel(censusData []CensusGroup) minmax{
    // var toReturn minmax;
    if len(censusData)<=10000{    
        var minimumLatitude  float64 = 9999;
        var minimumLongitude float64  = 9999;
        var maximumLatitude  float64 = -9999;
        var maximumLongitude float64  = -9999;
        for _,coords := range censusData{
            if coords.latitude < minimumLatitude {
                minimumLatitude = coords.latitude;
            }
            if coords.longitude < minimumLongitude {
                minimumLongitude = coords.longitude;
            }
            if coords.latitude > maximumLatitude {
                maximumLatitude = coords.latitude;
            }
            if coords.longitude > maximumLongitude {
                maximumLongitude = coords.longitude;
            }
        }
        return minmax{minimumLatitude, minimumLongitude, maximumLatitude, maximumLongitude};
    } else{
        mid := len(censusData)/2;
        done := make(chan bool);
        var toReturnLeft minmax;
        go func(){
            toReturnLeft = findminmax(censusData[:mid]);
            done<- true;
        }()
        toReturnRight := findminmax(censusData[mid:]);
        <-done;
        return minmax{math.Min(toReturnLeft.minimumLatitude, toReturnRight.minimumLatitude),
                      math.Min(toReturnLeft.minimumLongitude, toReturnRight.minimumLongitude),
                      math.Max(toReturnLeft.maximumLatitude, toReturnRight.maximumLatitude),
                      math.Max(toReturnLeft.maximumLongitude, toReturnRight.maximumLongitude)}

    }
}
func getQueryBlock(xdim, ydim int, long float64, lat float64, minmaxData minmax) pair {
    var sizeLongBlocks float64 = (minmaxData.maximumLongitude-minmaxData.minimumLongitude)/float64(xdim);
    var sizeLatBlocks float64 = (minmaxData.maximumLatitude-minmaxData.minimumLatitude)/float64(ydim);
    var x int = int(math.Ceil((long-minmaxData.minimumLongitude)/sizeLongBlocks));
    var y int = int(math.Ceil((lat-minmaxData.minimumLatitude)/sizeLatBlocks));
    return pair{x,y}
}
func getQueryBlockv3(xdim, ydim int, long float64, lat float64, minmaxData minmax) pair {
    var sizeLongBlocks float64 = (minmaxData.maximumLongitude-minmaxData.minimumLongitude)/float64(xdim);
    var sizeLatBlocks float64 = (minmaxData.maximumLatitude-minmaxData.minimumLatitude)/float64(ydim);
    var x int = int((long-minmaxData.minimumLongitude)/sizeLongBlocks);
    var y int = int((lat-minmaxData.minimumLatitude)/sizeLatBlocks);
    if x==xdim{
        x--;
    }
    if y==ydim{
        y--;
    }
    // fmt.Println(x,y);
    return pair{x,y}
}
func checkBlock(north, south, east, west int, x, y int) bool{
    if x>=west && x<=east && y>=south && y<=north{
        return true;
    }
    return false;
}
func queryv1(xdim, ydim int, minmaxData minmax, north, south, east, west int, censusData []CensusGroup) output{
    var block pair;
    sum := 0;
    totalsum := 0;
    for _, x := range censusData{
        block = getQueryBlock(xdim, ydim, x.longitude, x.latitude, minmaxData);
        totalsum += x.population;
        if (checkBlock(north, south, east, west, block.gridx, block.gridy)){
            sum+=x.population;
        }
    }
    var percentage float64 = (float64(sum)/float64(totalsum))*100.0;
    return output{sum, percentage};
}
func queryv2(xdim, ydim int, minmaxData minmax, north, south, east, west int, censusData []CensusGroup) output{
    if len(censusData)<=10000{
        var block pair;
        sum := 0;
        totalsum := 0;
        for _, x := range censusData{
            block = getQueryBlock(xdim, ydim, x.longitude, x.latitude, minmaxData);
            totalsum += x.population;
            if (checkBlock(north, south, east, west, block.gridx, block.gridy)){
                sum+=x.population;
            }
        }
        // var percentage float64 = (float64(sum)/float64(totalsum))*100.0;
        return output{sum, float64(totalsum)};
    } else {
        mid := len(censusData)/2;
        done := make(chan bool);
        var toReturnLeft output;
        go func(){
            toReturnLeft = queryv2(xdim, ydim, minmaxData, north, south, east, west, censusData[:mid]);
            done<- true;
        }()
        toReturnRight := queryv2(xdim, ydim, minmaxData, north, south, east, west, censusData[mid:]);
        <-done;
        return output{toReturnLeft.population+toReturnRight.population, float64(toReturnLeft.percentage+toReturnRight.percentage)}
    }
}
func createGridStep1(xdim, ydim int, minmaxData minmax, censusData []CensusGroup) [][]int{
    grid := make([][]int, ydim);
    for i:=0; i<(ydim); i++{
        grid[i] = make([]int, xdim);
    }
    var block pair;
    for _,x := range censusData{
        block = getQueryBlockv3(xdim, ydim, x.longitude, x.latitude, minmaxData)
        grid[block.gridy][block.gridx] = grid[block.gridy][block.gridx]+x.population;
        
    }
    return grid;
}
func createGridStep1parallel(xdim, ydim int, minmaxData minmax, censusData []CensusGroup) [][]int{
    if len(censusData)<=10000{
        grid := make([][]int, ydim);
        for i:=0; i<(ydim); i++{
            grid[i] = make([]int, xdim);
        }
        var block pair;
        for _,x := range censusData{
            block = getQueryBlockv3(xdim, ydim, x.longitude, x.latitude, minmaxData)
            grid[block.gridy][block.gridx] = grid[block.gridy][block.gridx]+x.population;
            
        }
        return grid;
    } else {
        mid := len(censusData)/2;
        done := make(chan bool);
        var toReturnLeft [][]int;
        go func(){
            toReturnLeft = createGridStep1parallel(xdim, ydim, minmaxData, censusData[:mid]);
            done<- true;
        }()
        toReturnRight := createGridStep1parallel(xdim, ydim, minmaxData, censusData[mid:]);
        <-done;
        return gridcopy(toReturnLeft, toReturnRight, xdim, ydim);
    }
}
func gridcopy(g1 [][]int, g2 [][]int, xdim, ydim int) [][]int{
    if xdim*ydim <=500{
        for x := range g1{
            for y := range g1[x]{
                g1[x][y] += g2[x][y];
            }
        }
        return g1;
    } else {
        //parallel copy
        done := make(chan bool);
        var toReturnLeft [][]int;
        go func(){
            toReturnLeft = gridcopy(g1[:ydim/2], g2[:ydim/2], xdim, ydim/2);
            done<- true;
        }()
        toReturnRight := gridcopy(g1[ydim/2:], g2[ydim/2:],xdim, ydim/2 );
        <-done;
        return append(toReturnLeft, toReturnRight...)
    }
}
func createGridStep2(grid [][]int) [][]int{
    updatedgrid := grid;
    for i,_ := range grid{
        for j,_ := range grid[i]{
            if ((i-1)>=0 && (j-1)>=0){
                updatedgrid[i][j] = updatedgrid[i][j] + updatedgrid[i-1][j] + updatedgrid[i][j-1] - updatedgrid[i-1][j-1];
            } else if ((i-1)<0 && (j-1)>=0){
                updatedgrid[i][j] = updatedgrid[i][j] + updatedgrid[i][j-1];
            } else if ((i-1)>=0 && (j-1)<0){
                updatedgrid[i][j] = updatedgrid[i][j] + updatedgrid[i-1][j];
            } else {
                updatedgrid[i][j] = updatedgrid[i][j];
            }
        }
    }
    return updatedgrid;
}
func transpose(grid [][]int)  [][]int{
	newgrid := make([][]int, len(grid[0]));
	for i,_ := range newgrid{
		newgrid[i] = make([]int, len(grid));
	}
	for y,s := range grid{
		for x,e := range s{
			newgrid[x][y] = e;
		}
	}
	return newgrid;
}
func PrefixSum(data, output []int, parent chan int) {
	if len(data) > 1 {
		mid := len(data)/2
		left := make(chan int)
		right := make(chan int)
		go PrefixSum(data[:mid], output[:mid], left)
		go PrefixSum(data[mid:], output[mid:], right)
		leftSum := <-left
		parent<- leftSum + <-right
		fromLeft := <-parent
		left<- fromLeft
		right<- fromLeft + leftSum















		<-left
		<-right
	} else if len(data) == 1 {
		parent<- data[0]
		output[0] = data[0] + <-parent
	} else {
		parent<- 0
		<-parent
	}
	parent<- 0
}
func createGridStep2parallel(grid [][]int) [][]int{
	updatedgrid := grid;
	for i,_ := range grid{
		data := grid[i];
		// output := make([]int, len(data))
		parent := make(chan int)
		go PrefixSum(data, updatedgrid[i], parent)
		<-parent  // sum
		fromLeft := 0
		parent<- fromLeft
		<-parent   // doneZero
		// fmt.Printf("%v\n%v\n", data, output)
	}
	updatedgrid = transpose(updatedgrid);
	updatedgridnew := updatedgrid;
	// fmt.Println("here?");
	for i,_ := range updatedgrid{
		data := updatedgrid[i];
		parent := make(chan int);
		go PrefixSum(data, updatedgridnew[i], parent);
		<-parent;
		fromLeft := 0;
		parent<-fromLeft;
		<-parent;
	}
	return transpose(updatedgridnew);
}
func queryv3(xdim, ydim int, minmaxData minmax, north, south, east, west int, grid [][]int) output{
    startx := west-1;
    endx := east-1;
    starty := south-1
    endy := north-1;
    sum := grid[endy][endx]
    if (starty-1)>0{
        sum -= grid[starty-1][endx];
    }
    if (startx-1)>0{
        sum -= grid[endy][startx-1];
    }
    if (startx-1)>0 && (starty-1)>0{
        sum -= grid[starty-1][startx-1];
    }
    var percentage float64 = (float64(sum)/float64(grid[ydim-1][xdim-1]))*100.0;
    return output{sum,percentage};
}
func singleInputOutput(ver string, xdim, ydim int, minmaxes minmax, singleInput direction, updatedgrid [][]int, censusData []CensusGroup){
    var population int;
    var percentage float64;
    switch ver {
    case "-v1":
        x := queryv1(xdim, ydim, minmaxes, singleInput.north, singleInput.south, singleInput.east, singleInput.west, censusData);
        population = x.population;
        percentage = x.percentage;
    case "-v2":
        y := queryv2(xdim, ydim, minmaxes, singleInput.north, singleInput.south, singleInput.east, singleInput.west, censusData);
        
        population = y.population;
        percentage = (float64(population)/y.percentage)*100.0;
    case "-v3":
        x := queryv3(xdim, ydim, minmaxes, singleInput.north, singleInput.south, singleInput.east, singleInput.west, updatedgrid);
        population = x.population;
        percentage = x.percentage;
    case "-v4":
    	x := queryv3(xdim, ydim, minmaxes, singleInput.north, singleInput.south, singleInput.east, singleInput.west, updatedgrid);
    	population = x.population;
    	percentage = x.percentage;
    case "-v5":
    case "-v6":
    	// fmt.Println("lil kappa");
    	x := queryv3(xdim, ydim, minmaxes, singleInput.north, singleInput.south, singleInput.east, singleInput.west, updatedgrid);
    	population = x.population;
    	percentage = x.percentage;
    }

    fmt.Printf("%v %.2f%%\n", population, percentage);
}
func parallelInputOutput(ver string, xdim, ydim int, minmaxes minmax, allInputs []direction, updatedgrid [][]int, censusData []CensusGroup){
    if len(allInputs) == 1{
    	singleInputOutput(ver, xdim, ydim, minmaxes, allInputs[0], updatedgrid, censusData);
    }else{
	    done := make(chan bool);
	    mid := len(allInputs)/2;
	    go func(){
	    	parallelInputOutput(ver, xdim, ydim, minmaxes, allInputs[:mid], updatedgrid, censusData);
	    	done<-true;
	    }()
    	parallelInputOutput(ver, xdim, ydim, minmaxes, allInputs[mid:], updatedgrid, censusData);
    	<-done;
	}
}
func main () {
	if len(os.Args) < 4 {
		fmt.Printf("Usage:\nArg 1: file name for input data\nArg 2: number of x-dim buckets\nArg 3: number of y-dim buckets\nArg 4: -v1, -v2, -v3, -v4, -v5, or -v6\n")
		return
	}
	
	// fmt.Println("Launching Population Query Server");
	// ln, _ := net.Listen("tcp", ":8081");
	// conn, _ := ln.Accept();

	// message, _ := bufio.NewReader(conn).ReadString('\n');
	// fmt.Print("Stuff received: ", string(message))


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
    var minmaxes minmax;
    var updatedgrid [][]int;
    globalgrid := make([][]int, ydim);
        for i:=0;i<ydim;i++{
            globalgrid[i] = make([]int, xdim);
        }
    switch ver {
    case "-v1":
        minmaxes = findminmax(censusData);
    case "-v2":
        minmaxes = findminmaxparallel(censusData);
    case "-v3":
        minmaxes = findminmax(censusData)
        grid := createGridStep1(xdim, ydim, minmaxes, censusData);
        updatedgrid = createGridStep2(grid);
    case "-v4":
    	minmaxes = findminmaxparallel(censusData);
    	grid := createGridStep1parallel(xdim, ydim, minmaxes, censusData);
    	updatedgrid = createGridStep2(grid);

    case "-v5":
    case "-v6":
    	minmaxes = findminmaxparallel(censusData);
    	grid := createGridStep1parallel(xdim, ydim, minmaxes, censusData);
    	updatedgrid = createGridStep2parallel(grid);
   
    	// fmt.Println("lul kappa")
    default:
        fmt.Println("Invalid version argument")
        return
    }
    // fmt.Println("Enter -1 when done");
    allInputs := make([]direction, 0);
    // var west, south, east, north int
    for {
        var west, south, east, north int
        n, err := fmt.Scanln(&west, &south, &east, &north)
        if n != 4 || err != nil || west<1 || west>xdim || south<1 || south>ydim || east<west || east>xdim || north<south || north>ydim {
            break
        }
    allInputs = append(allInputs, direction{west, south, east, north});
    }
    // xyvalues := strings.Split(message, " ");
    // west, _ = strconv.Atoi(xyvalues[0]);
    // south, _ = strconv.Atoi(xyvalues[1]); 
    // east, _ = strconv.Atoi(xyvalues[2]); 
    // north, _ = strconv.Atoi(xyvalues[3]);
    // fmt.Println(west, south, east, north);
    // fmt.Println(xyvalues);
    parallelInputOutput(ver, xdim, ydim, minmaxes, allInputs, updatedgrid, censusData);
        // var population int
        // var percentage float64
        // switch ver {
        // case "-v1":
        //     x := queryv1(xdim, ydim, minmaxes, north, south, east, west, censusData);
        //     population = x.population;
        //     percentage = x.percentage;
        // case "-v2":
        //     y := queryv2(xdim, ydim, minmaxes, north, south, east, west, censusData);
            
        //     population = y.population;
        //     percentage = (float64(population)/y.percentage)*100.0;
        // case "-v3":
        //     x := queryv3(xdim, ydim, minmaxes, north, south, east, west, updatedgrid);
        //     population = x.population;
        //     percentage = x.percentage;
        // case "-v4":
        // 	x := queryv3(xdim, ydim, minmaxes, north, south, east, west, updatedgrid);
        // 	population = x.population;
        // 	percentage = x.percentage;
        // case "-v5":
        // case "-v6":
        // 	fmt.Println("lil kappa");
        // 	x := queryv3(xdim, ydim, minmaxes, north, south, east, west, updatedgrid);
        // 	population = x.population;
        // 	percentage = x.percentage;
        // }

        // fmt.Printf("%v %.2f%%\n", population, percentage)
    
}
