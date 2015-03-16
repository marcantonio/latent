
package main

import (
	"fmt"
	"os"
	"net"
	"time"
	"strconv"
	"sync"
	"io"
	"encoding/json"
	"net/http"
	"io/ioutil"
	"strings"
	"encoding/csv"
	"sort"
)

/*
 * Improvements:
 *
 * - Use a config file
 * - Self distribute (via ssh)
 * - Command channel for shutdown
 * - Pool connections
 * - Command to change master node
 * - Better organization
 */

// Static server list
var server_list = map[string]string{
	"us-va": "52.1.134.148",
	"us-or": "52.10.155.112",
	"us-ca": "54.153.89.227",
	"eu-ir": "52.16.214.187",
	"eu-fr": "54.93.200.30",
	"ap-sp": "52.74.73.240",
	"ap-tk": "54.65.243.9",
	"ap-sy": "52.64.39.254",
	"sa-sp": "54.94.249.234",
}

// Node seen as master
const masterId = "us-va"

const outputFile = "latency-report.csv"

type Server struct {
	id string
	ip string
	results []time.Duration
	average float64
}

// Find the average for all results and convert is to ms.
func (s *Server) calcResult() {
	var total float64
	for _, v := range s.results {
		total += float64(v.Nanoseconds()) / 1000000
	}
	s.average = total / float64(len(s.results))
}

type Result struct {
	Id string
	Average string
}

type ResultSet struct {
	Id string
	Results []Result
}

func main() {
	// Get my ID.
	myId := whoAmI()
	
	// Get ride of old output.
	os.Remove(outputFile)
	
	// Launch the thread that gathers all of our metrics.
	go gatherers(myId)
	
	// Fire up a listener to echo back and send reports.
	tcpAddr, err := net.ResolveTCPAddr("tcp", ":80")
	checkErrorFatal(err)
	
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkErrorFatal(err)
	
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error accepting conn: %s", err.Error())
		}
		// Launch a thread to do the stuff.
		go handleConn(conn)
	}
}

// Fatal errors die here.
func checkErrorFatal(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %s ", err.Error())
		os.Exit(1)
	}
}

/*
 * This function discovers the node's ID by determining its external
 * IP.  Right now, we hit ifconfig.me for our external IP.  This is
 * less than desirable as it's a (frequently slow) external service.
 * We should use a simple STUN server here.
 */
func whoAmI() string {
	fmt.Println("Finding myself...")
	
	// Get our IP with a custom timeout.  We can be rid of this when
	// we're using something other than ifconfig.me.
	transport := http.Transport{
        Dial: discoverTimeout,
	}
	client := &http.Client{
		Transport: &transport,
	}
	
	req, err := http.NewRequest("GET", "http://ifconfig.me", nil)
	checkErrorFatal(err)
	
	// This is important for ifconfig.me.  If it thinks we're curl it
	// only return our IP rather than the whole page.
	req.Header.Set("User-Agent", "curl/7.35.0")
	
	resp, err := client.Do(req)
	checkErrorFatal(err)
	defer resp.Body.Close()
	
	body, err := ioutil.ReadAll(resp.Body)
	checkErrorFatal(err)
	
	myIp := strings.TrimSpace(string(body))
	
	// Find our ID.
	var myId string
	for id, ip := range server_list {
		if ip == myIp {
			myId = id
			break
		}
	}
	
	if myId != "" {
		fmt.Println("I am: " + myId)
	} else {
		fmt.Fprintf(os.Stderr, "Error: Can't determine my ID ")
		os.Exit(1)
	}
	return myId
}

// Custom timeout getting our IP.
func discoverTimeout(network, addr string) (net.Conn, error) {
    return net.DialTimeout(network, addr, time.Duration(30 * time.Second))
}

/*
 * Handles all connections from other nodes.  We do one of two things:
 * either echo back the timestamp in the message, or if it's JSON,
 * treat that as a report from anoter node.
 */
func handleConn(conn net.Conn) {
	defer conn.Close()
	
	buf := make([]byte, 1024)
	
	c, err := conn.Read(buf[0:])
	if err != nil {
		if err == io.EOF {
			return
		}
		fmt.Fprintf(os.Stderr, "Error reading: %s", err.Error())
		return
	}
	
	// If the first character is a '{' we assume it's JSON.  Decode it and send it for outputting.
	if buf[0] == '{' {
		var rs ResultSet
		err := json.Unmarshal(buf[:c], &rs)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error parsing result: %s", err.Error())
		}
		outputResults(&rs)
	}
	
	// If it's a timestamp just echo it back to the client.
	_, err = conn.Write(buf[:c])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing: %s", err.Error())
		return
	}
}

// Take the ResultSet and output it to STDOUT and a CSV.
// TODO: Clean this mess up...
func outputResults(rs *ResultSet) {
	// Open the file and output the header if necessary.
	var file *os.File
	// Since we get called for every ResultSet, figure out if the
	// output file exists or not and create or open for appending as
	// appropriate.
	if _, err := os.Stat(outputFile); err != nil {
		if os.IsNotExist(err) {
			fmt.Println("Saving to " + outputFile)
			file, err = os.OpenFile(outputFile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
			checkErrorFatal(err)
			
			// Get the header together
			header := make([]string, len(rs.Results)+1)
			header[0] = "Id"
			for n, result := range rs.Results {
				header[n+1] = result.Id
			}
			sort.Strings(header)
			
			writer := csv.NewWriter(file)
			err = writer.Write(header)
			checkErrorFatal(err)
			writer.Flush()
		} else {
			checkErrorFatal(err)
		}
	} else {
		file, err = os.OpenFile(outputFile, os.O_APPEND|os.O_WRONLY, 0600)
		checkErrorFatal(err)
	}
	defer file.Close()
	
	// Output to STDOUT and build record map.
	fmt.Println("Results from: " + rs.Id)
	record := make(map[string]string, len(rs.Results)+1)
	record["Id"] = rs.Id
	
	for _, result := range rs.Results {
		fmt.Printf("  %s: %s\n", result.Id, result.Average)
		record[result.Id] = result.Average
	}
	
	// Sort the results.
	sortedRecord := sortResults(&record)
	
	// Write to a CSV.
	writer := csv.NewWriter(file)
	err := writer.Write(sortedRecord)
	checkErrorFatal(err)
	writer.Flush()
}

// Sort???  Go...
func sortResults(record *map[string]string) []string {
	var keys []string
	for k := range *record {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sortedRecord []string
	for _, k := range keys {
		sortedRecord = append(sortedRecord, (*record)[k])
	}
	return sortedRecord
}

// This is the client side.  We make sure all of our nodes are up then
// start gather stats.  When that's done we send all of our results to
// the master node.
func gatherers(myId string) {
	num_samples := 10
	servers := make([]Server, 0)
	
	// Block while all nodes come to life.  Inc wg for each node then
	// dec with each thread on connect.  Blocks until *ALL* are up.
	var wg sync.WaitGroup
	for id, ip := range server_list {
		server := Server{id:id, ip:ip}
		servers = append(servers, server)
		wg.Add(1)
		go func() {
			defer wg.Done()
			tcpAddr, err := net.ResolveTCPAddr("tcp", server.ip + ":80")
			checkErrorFatal(err)
			for {
				conn, err := net.DialTCP("tcp", nil, tcpAddr)
				if err == nil {
					fmt.Printf("Server: %s joined\n", server.id);
					conn.Close()
					return
				} else {
					time.Sleep(500 * time.Millisecond)
					continue
				}
			}
		}()
	}
	wg.Wait()
	fmt.Println("All servers up");
	
	// Take one measurement for each node, num_samples times, 100ms apart.
	var masterIdx int
	for x := range servers {
		fmt.Printf("Collecting for: %v\n", servers[x].id)
		results := make([]time.Duration, 0)
		for n := 0; n < num_samples; n++ {
			time.Sleep(100 * time.Millisecond)
			results = append(results, measure(&(servers[x])))
		}
		servers[x].results = results
//		report(results)
		// Get our average.
		servers[x].calcResult()
		// Find the master node.
		if servers[x].id == masterId {
			masterIdx = x
		}
//		fmt.Printf("  %0.3fms\n", servers[x].average)
	}
	// Ship the results to the master.
	sendResults(&servers[masterIdx], &servers, myId)
}

// This is the bulk of the client.  We connect to the server and send
// the current time, in nanoseconds.  It echoes it back and we call
// the delta latency.
func measure(server *Server) time.Duration {
	tcpAddr, err := net.ResolveTCPAddr("tcp", server.ip + ":80")
	checkErrorFatal(err)
	
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkErrorFatal(err)
	defer conn.Close()
	
	// Write
	_, err = conn.Write([]byte(strconv.Itoa(int(time.Now().UnixNano()))))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing: %s", err.Error())
		return 0
	}
	
	// Read
	var buf [1024]byte
	c, err := conn.Read(buf[0:])
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error reading: %s", err.Error())
		return 0
	}
	
	ts, err := strconv.Atoi(string(buf[:c]))
	checkErrorFatal(err)
	
	// Find delta
	result := time.Now().Sub(time.Unix(0, int64(ts)))
	return result
}

// Sends all our results to the master node as JSON.  This is the last step.
func sendResults(master *Server, servers *[]Server, myId string) {
	fmt.Println("Sending results to: " + master.id)
	tcpAddr, err := net.ResolveTCPAddr("tcp", master.ip + ":80")
	checkErrorFatal(err)
	
	conn, err := net.DialTCP("tcp", nil, tcpAddr)
	checkErrorFatal(err)
	defer conn.Close()
	
	results := make([]Result, 0)
	for _, server := range *servers {
		r := Result{}
		r.Id = server.id
		r.Average = fmt.Sprintf("%0.3fms", server.average)
		results = append(results, r)
	}
	
	rs := ResultSet{myId, results}
	j, err := json.Marshal(rs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error building results: %s", err.Error())		
	}
	
	_, err = conn.Write(j)
	checkErrorFatal(err)
}

// For debugging.
func report(results []time.Duration) {
	var total float64
	for _, v := range results {
		fmt.Printf("%0.3fms\n", float64(v.Nanoseconds())/1000000)
		total += float64(v.Nanoseconds()) / 1000000
	}
	fmt.Printf(">%0.3fms\n", total / float64(len(results)))
}
