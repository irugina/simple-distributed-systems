package main

import (
	"fmt"
	"sync"
)

type Fetcher interface {
	// Fetch returns the body of URL and
	// a slice of URLs found on that page.
	Fetch(url string) (body string, urls []string, err error)
}






// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func CrawlSerial(url string, depth int, fetcher Fetcher, fetched map[string]bool) {
	if fetched[url] {
		return
	}
	fetched[url]=true
	if depth <= 0 {
		return
	}
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("found: %s %q\n", url, body)
	for _, u := range urls {
		CrawlSerial(u, depth-1, fetcher, fetched)
	}
	return
}








// struct for concurrent web crawler with locks
type fetchState struct {
	mu      sync.Mutex
	fetched map[string]bool
}
// init new struct
func makeState() *fetchState {
	f := &fetchState{}
	f.fetched = make(map[string]bool)
	return f
}
// Crawl uses fetcher to recursively crawl
// pages starting with url, to a maximum of depth.
func CrawlLocks(url string, depth int, fetcher Fetcher, f *fetchState) {
	f.mu.Lock()
	seen := f.fetched[url]
	f.fetched[url] = true
	f.mu.Unlock()
	if seen {
		return
	}
	if depth <= 0 {
		return
	}
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		return
	}
	var done sync.WaitGroup
	fmt.Printf("found: %s %q\n", url, body)
	for _, u := range urls {
		done.Add(1)
		go func(u string) {
			defer done.Done()
			CrawlLocks(u, depth-1, fetcher, f)
		} (u) //pass u as arg so that anon fn will use this loop's u
	}
	done.Wait()
	return
}








func worker(url string, ch chan []string, fetcher Fetcher){
	body, urls, err := fetcher.Fetch(url)
	if err != nil {
		fmt.Println(err)
		ch <- []string{}
	} else{
		fmt.Printf("found: %s %q\n", url, body)
		ch <- urls
	}
}
func coordinator(ch chan []string, fetcher Fetcher){
	n := 1
	fetched := make(map[string]bool)
	for urls := range ch{ //keep reading the channel
		for _, u := range urls {
			if fetched[u] == false{
				fetched[u] = true
				n += 1
				go worker(u, ch, fetcher)
			}
		}
		n -= 1
		if n == 0{
			break
		}
	}
}


func CrawlChannel(url string, fetcher Fetcher){
	ch:= make (chan []string)
	go func (){
		ch <- []string {url}
	} ()
	coordinator(ch, fetcher)
}






func main() {
	//CrawlSerial("https://golang.org/", 4, fetcher, make(map[string]bool))
	//CrawlLocks("https://golang.org/", 4, fetcher, makeState())
	CrawlChannel("https://golang.org/", fetcher)
}

// fakeFetcher is Fetcher that returns canned results.
type fakeFetcher map[string]*fakeResult

type fakeResult struct {
	body string
	urls []string
}

func (f fakeFetcher) Fetch(url string) (string, []string, error) {
	if res, ok := f[url]; ok {
		return res.body, res.urls, nil
	}
	return "", nil, fmt.Errorf("not found: %s", url)
}

// fetcher is a populated fakeFetcher.
var fetcher = fakeFetcher{
	"https://golang.org/": &fakeResult{
		"The Go Programming Language",
		[]string{
			"https://golang.org/pkg/",
			"https://golang.org/cmd/",
		},
	},
	"https://golang.org/pkg/": &fakeResult{
		"Packages",
		[]string{
			"https://golang.org/",
			"https://golang.org/cmd/",
			"https://golang.org/pkg/fmt/",
			"https://golang.org/pkg/os/",
		},
	},
	"https://golang.org/pkg/fmt/": &fakeResult{
		"Package fmt",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
	"https://golang.org/pkg/os/": &fakeResult{
		"Package os",
		[]string{
			"https://golang.org/",
			"https://golang.org/pkg/",
		},
	},
}

