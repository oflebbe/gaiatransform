package main

import (
	"bufio"
	"compress/gzip"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
)

// ReadFiles read filenames from chanel ch and puts them on chanel output
func ReadFiles(ch *chan string, cols []int, output *chan []float32, wg *sync.WaitGroup) {
	defer wg.Done()
	for s := range *ch {
		ReadOneFile(s, cols, output)
		fmt.Printf("%s\n", s)
	}
	fmt.Printf("End of Jobs\n")
}

// ReadOneFile reads on file
func ReadOneFile(fn string, cols []int, ch *chan []float32) {
	file, err := os.Open(fn)
	if err != nil {
		log.Fatalf("Could not open %s", fn)
	}
	uncompressor, err := gzip.NewReader(file)
	if err != nil {
		log.Fatalf("Could not uncompress %s", fn)
	}
	scanner := bufio.NewScanner(uncompressor)
	scanner.Scan() // skip first line

	var result []float32

	buffer := make([]float32, len(cols), len(cols))
	for scanner.Scan() {
		line := scanner.Text()
		toks := strings.Split(line, ",")
		for i, v := range cols {
			if toks[v] == "" {
				buffer[i] = float32(math.NaN())
			}
			val, err := strconv.ParseFloat(toks[v], 32)
			if err != nil {
				log.Fatalf("line %s not parsable: ra", line)
			}
			buffer[i] = float32(val)
		}

		result = append(result, buffer...)
	}
	*ch <- result
	return
}

func main() {
	numArgs := len(os.Args)
	var cols []int
	for i := 2; i < numArgs; i++ {
		icol, err := strconv.Atoi(os.Args[i])
		if err != nil {
			log.Fatal("columns not int")
		}
		cols = append(cols, icol)
	}

	fileInfo, err := ioutil.ReadDir(os.Args[1])
	if err != nil {
		log.Fatalf("Could not read %s", os.Args[1])
	}
	result, err := os.Create("result.dat")
	if err != nil {
		log.Fatalf("COuld not create %s", "result.dat")
	}
	defer result.Close()

	ch := make(chan []float32)
	count := 0

	fileNameCh := make(chan string)
	var wg sync.WaitGroup
	end := make(chan int64)
	// Writer
	go func(ch *chan []float32, end *chan int64) {
		var countResults int64
		var countCluster int64
		for coords := range *ch {
			countCluster++
			countResults += int64(len(coords))
			binary.Write(result, binary.LittleEndian, coords)
		}
		result.Close()
		*end <- countResults
		*end <- countCluster
		close(*end)

	}(&ch, &end)

	// Reader
	for i := 0; i < runtime.NumCPU(); i++ {
		go func(wg *sync.WaitGroup) {
			wg.Add(1)
			ReadFiles(&fileNameCh, cols, &ch, wg)
		}(&wg)
	}

	// Dispatch Readers
	for _, fi := range fileInfo {
		if filepath.Ext(fi.Name()) != ".gz" {
			continue
		}
		count++
		fileNameCh <- filepath.Join(os.Args[1], fi.Name())
	}
	close(fileNameCh)
	fmt.Printf("Starting Wait\n")
	wg.Wait()
	close(ch)
	fmt.Printf("End of Wait\n")
	fmt.Printf("%d Results\n", <-end)
	fmt.Printf("%d Cluster\n", <-end)
}
