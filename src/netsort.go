package main

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

type KVpair struct {
	Key   []byte
	Value []byte
}

var receivedList []KVpair

func readServerConfigs(configPath string) ServerConfigs {
	f, err := ioutil.ReadFile(configPath)

	if err != nil {
		log.Fatalf("could not read config file %s : %v", configPath, err)
	}

	scs := ServerConfigs{}
	err = yaml.Unmarshal(f, &scs)

	return scs
}

func main() {
	/*
	 * mynotes: every server has to perform this function respecetively,
	 * we only need to know the serverId, and listen for the others.
	 */

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	if len(os.Args) != 5 {
		log.Fatal("Usage : ./netsort {serverId} {inputFilePath} {outputFilePath} {configFilePath}")
	}

	// What is my serverId
	serverId, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatalf("Invalid serverId, must be an int %v", err)
	}
	fmt.Println("My server Id:", serverId)

	// Read server configs from file
	scs := readServerConfigs(os.Args[4])
	fmt.Println("Got the following server configs:", scs)
	/*
		Implement Distributed Sort
	*/

	var inputfile string = os.Args[2]
	var outputfile string = os.Args[3]

	/*
	 * Spawn a new goroutine that concurrently listens for connections from
	 * clients and pass the send side of this channel to this go-routine.
	 */

	// current ip & listen for this ip
	service := scs.Servers[serverId].Host + ":" + scs.Servers[serverId].Port
	fmt.Printf("Now server is %s \n ", service)

	// create map to store arranged data before sending
	map_data := make(map[int][]KVpair)
	server_bits_number := PowerOfTwo(len(scs.Servers))
	// fmt.Printf("server_bits_number: %d \n", server_bits_number)
	arrangingLocalData(inputfile, map_data, server_bits_number)

	listener, err := net.Listen("tcp", service)
	if err != nil {
		log.Fatal("Listening Error: ", err.Error())
		os.Exit(1)
	}
	defer listener.Close()

	var mutex sync.Mutex
	var wg_server sync.WaitGroup
	wg_server.Add(len(scs.Servers))

	// fmt.Println("Before Wait")
	go func() {

		defer wg_server.Done()

		listeningConn(listener, mutex, scs)
	}()

	for i := 0; i < len(scs.Servers); i++ {
		if i == serverId {
			continue
		}

		other_server := scs.Servers[i].Host + ":" + scs.Servers[i].Port
		other_server_id := scs.Servers[i].ServerId

		go func() {
			defer wg_server.Done()

			conn, err := net.Dial("tcp", other_server)
			for err != nil {
				time.Sleep(time.Duration(50) * time.Microsecond)
				conn, err = net.Dial("tcp", other_server)
			}
			defer conn.Close()
			// fmt.Printf("Length of map: %d \n", len(map_data[other_server_id]))
			sendingData(conn, map_data[other_server_id])
		}()
	}
	wg_server.Wait()
	// fmt.Printf("After Wait, %d \n", len(receivedList))

	for _, message := range map_data[serverId] {
		receivedList = append(receivedList, message)
	}
	// fmt.Printf("--------------Length of receivedList: %d \n", len(receivedList))
	//process the message we got
	sortTheMessages(receivedList, outputfile)
	fmt.Printf("-------Server %s Ending------ \n", os.Args[1])
}

func checkError(err error) {
	if err != nil {
		log.Fatalf("Fatal error: %s", err.Error())
	}
}

func arrangingLocalData(inputfile string, map_data map[int][]KVpair, server_bits int) {
	f, err := os.Open(inputfile)
	if err != nil {
		log.Fatalln("Fatal error: openFile")
	}
	defer f.Close()

	for {
		msg_buf := make([]byte, 100)
		_, err := f.Read(msg_buf)
		if err != nil {
			if err == io.EOF {
				break
			}
		}
		client_number := extractBits(msg_buf, server_bits)
		var kv KVpair
		kv.Key = append(kv.Key, msg_buf[:10]...)
		kv.Value = append(kv.Value, msg_buf[10:100]...)
		// fmt.Printf("Arrange: client_number: %d, Key: %s \n", client_number, hex.EncodeToString(kv.Key))
		map_data[client_number] = append(map_data[client_number], kv)
		// fmt.Printf("After appending, Server %s - %d length of map_data: %d \n", os.Args[1], client_number, len(map_data[client_number]))
	}

}

func listeningConn(listener net.Listener, mutex sync.Mutex, scs ServerConfigs) {
	for i := 0; i < len(scs.Servers)-1; i++ {
		conn, err := listener.Accept()
		if err != nil {
			// server closed
			break
		}
		receivingData(conn, mutex)
	}
}

func writeNBytes(connection net.Conn, buffer []byte, length int) {
	cnt, _ := connection.Write(buffer)
	for cnt < length {
		n, _ := connection.Write(buffer[cnt:])
		cnt += n
	}
}

func readNBytes(connection net.Conn, buffer []byte, length int) {
	cnt, _ := connection.Read(buffer[0:])
	for cnt < length {
		n, err := connection.Read(buffer[cnt:])
		if err != nil && err != io.EOF {
			break
		}
		cnt += n
	}
}

func sendingData(conn net.Conn, messages []KVpair) {
	for _, message := range messages {
		send_buf := make([]byte, 1)
		send_buf[0] = byte(0)
		send_buf = append(send_buf, message.Key...)
		send_buf = append(send_buf, message.Value...)
		// fmt.Printf("client_number: %s, sending Key: %s \n", os.Args[1], hex.EncodeToString(message.Key))
		writeNBytes(conn, send_buf, 101)
	}
	send_end := make([]byte, 1)
	send_end[0] = byte(1)
	writeNBytes(conn, send_end, 1)
}

func receivingData(conn net.Conn, mutex sync.Mutex) {
	isValid := make([]byte, 1)
	message := make([]byte, 100)
	conn.Read(isValid)

	for isValid[0] == byte(0) {
		readNBytes(conn, message, 100)

		var kvpair KVpair
		kvpair.Key = append(kvpair.Key, message[:10]...)
		kvpair.Value = append(kvpair.Value, message[10:100]...)
		// fmt.Printf("Client number: %s, received: %s \n", os.Args[1], hex.EncodeToString(kvpair.Key))

		mutex.Lock()
		receivedList = append(receivedList, kvpair)
		// fmt.Printf("After receiving, length of receivedList: %d \n", len(receivedList))
		mutex.Unlock()
		conn.Read(isValid)
	}

}

func PowerOfTwo(num int) int {
	count := 0
	for num > 0 {
		if num%2 == 0 {
			count++
			num = num / 2
		} else {
			break
		}
	}
	return count
}

func sortTheMessages(message []KVpair, outputfile string) {

	sort.Slice(message, func(i, j int) bool { return bytes.Compare(message[i].Key, message[j].Key) < 0 })

	f, err := os.Create(outputfile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	for _, element := range message {
		_, err := f.Write(element.Key)
		if err != nil {
			log.Fatal(err)
		}
		_, err = f.Write(element.Value)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func extractBits(buf []byte, numBits int) int {
	result := int(buf[0] >> (8 - numBits))
	return result
}
