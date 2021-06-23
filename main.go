package main

import (
	"flag"
	"github.com/nyudlts/go-aspace"
	"log"
	"sync"
)

var (
	client *aspace.ASClient
	err error
	repoId int
	numChunks int
	environment string
	config string
)

func init(){
	flag.IntVar(&repoId, "repository", 0, "repository id")
	flag.IntVar(&numChunks, "chunks", 2, "number of chunks")
	flag.StringVar(&environment, "environment", "dev", "environment")
	flag.StringVar(&config, "config", "go-aspace.yml", "config file")
}

func main() {
	flag.Parse()
	client, err = aspace.NewClient(config, environment, 20)
	if err != nil {
		panic(err)
	}
	log.Println("go-aspace", aspace.LibraryVersion)
	doids, err := client.GetDigitalObjectIDs(repoId)
	if err != nil {
		panic(err)
	}

	log.Println("Number of doids", len(doids))

	chunksize := len(doids) / numChunks
	doidChunks := chunks(doids, chunksize)

	c := make(chan []string)
	wg := sync.WaitGroup{}
	wg.Add(len(doidChunks))

	for i, chunk := range doidChunks {
		go GetDOs(chunk, c, i)
	}

	uris := []string{}
	for range doidChunks {
		chunk := <-c
		log.Println("Adding", len(chunk), "uris to uri list")
		uris = append(uris, chunk...)
	}

	log.Println(len(uris) == len(doids))

}

func GetDOs(doids []int, c chan []string, count int) {

	log.Println("Starting worker", count + 1, "processing", len(doids), "digital objects")

	uris := []string{}
	for _, doid := range doids {
		//fmt.Print(" ", count+1)
		do, err := client.GetDigitalObject(repoId, doid)
		if err != nil {}
		uris = append(uris, do.URI)
	}

	log.Println("worker", count + 1, "done")
	c <- uris

}

func chunks(xs []int, chunkSize int) [][]int {
	if len(xs) == 0 {
		return nil
	}
	divided := make([][]int, (len(xs)+chunkSize-1)/chunkSize)
	prev := 0
	i := 0
	till := len(xs) - chunkSize
	for prev < till {
		next := prev + chunkSize
		divided[i] = xs[prev:next]
		prev = next
		i++
	}
	divided[i] = xs[prev:]
	return divided
}
