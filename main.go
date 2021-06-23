package main

import (
	"fmt"
	"github.com/nyudlts/go-aspace"
	"sync"
)

var (
	client *aspace.ASClient
	err error
	repoId int = 6
	numChunks = 10
)

func main() {
	client, err = aspace.NewClient("go-aspace.yml", "dev", 20)
	if err != nil {
		panic(err)
	}

	doids, err := client.GetDigitalObjectIDs(repoId)
	if err != nil {
		panic(err)
	}

	fmt.Println("Number of doids", len(doids))

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
		fmt.Println("Adding", len(chunk), "uris to uri list")
		uris = append(uris, chunk...)
	}

	fmt.Println(len(uris) == len(doids))

}

func GetDOs(doids []int, c chan []string, count int) {

	fmt.Println("Starting worker", count + 1, "processing", len(doids), "digital objects")

	uris := []string{}
	for _, doid := range doids {
		do, err := client.GetDigitalObject(repoId, doid)
		if err != nil {}
		uris = append(uris, do.URI)
	}

	fmt.Println("worker", count + 1, "done")
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
