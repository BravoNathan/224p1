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

	"gopkg.in/yaml.v2"
)

type ServerConfigs struct {
	Servers []struct {
		ServerId int    `yaml:"serverId"`
		Host     string `yaml:"host"`
		Port     string `yaml:"port"`
	} `yaml:"servers"`
}

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

	// Create a bi-directional channel
	bidirectional_ch := make(chan []byte)
	defer close(bidirectional_ch)

	/*
	 * Spawn a new goroutine that concurrently listens for connections from
	 * clients and pass the send side of this channel to this go-routine.
	 */

	// current ip & listen for this ip
	service := scs.Servers[serverId].Host + ":" + scs.Servers[serverId].Port
	fmt.Printf("Now server is %s \n ", service)

	listener, err := net.Listen("tcp", service)
	checkError(err)

	var wg_server sync.WaitGroup
	wg_server.Add(len(scs.Servers) - 1)

	for i := 0; i < len(scs.Servers); i++ {
		if i == serverId {
			continue
		}

		other_server := scs.Servers[i].Host + ":" + scs.Servers[i].Port
		other_server_id := scs.Servers[i].ServerId

		go func() {
			defer wg_server.Done()

			var conn net.Conn
			for {
				conn1, err := net.Dial("tcp", other_server)
				if err == nil {
					conn = conn1
					break
				}
			}
			defer conn.Close()

			fmt.Printf("Server %d Connected to Server %d \n", serverId, other_server_id)
			server_number := len(scs.Servers)
			// fmt.Printf("server_number: %d \n", server_number)
			bytes_for_server := PowerOfTwo(server_number)
			fmt.Printf("bytes_for_server: %d \n", bytes_for_server)

			f, err := os.Open(inputfile)
			checkError(err)
			defer f.Close()

			for {
				// read key-value pair from the file
				client_msg_buf := make([]byte, 100)
				bytes_read, err := f.Read(client_msg_buf)
				if err != nil && err != io.EOF {
					log.Fatal(err)
				}

				if err == io.EOF {
					break
				}
				/*
					get the first "bytes_for_server" bytes and if it matches the current client_id,
					write to the conn
				*/

				fmt.Println("Bug_1")

				client_number_byte := make([]byte, bytes_for_server)

				// fmt.Printf("Length of client_msg_buf %d \n", len(client_msg_buf))

				copy(client_number_byte, client_msg_buf[:bytes_for_server])

				// fmt.Printf("Length of client_msg_buf[:bytes_for_server] %d \n", len(client_msg_buf[:bytes_for_server]))

				// fmt.Printf("Length of client_number_byte %d \n", len(client_number_byte))

				// fmt.Println("Bug_2")
				fmt.Printf("client_number_byte: %b", client_number_byte)
				client_number := extractBits(client_msg_buf, bytes_for_server)
				fmt.Printf("client_number: %d \n", client_number)

				if client_number == other_server_id {
					fmt.Printf("Writing key-value to %d conn \n", client_number)
					stream_complete := []byte{0}
					client_msg := client_msg_buf[0:bytes_read]
					message := append(stream_complete, client_msg...)
					fmt.Printf("Length of message: %d \n", len(message))
					conn.Write(message)
				}
			}
			end_byte_stream := make([]byte, 100)
			stream_complete := []byte{1}
			message := append(stream_complete, end_byte_stream...)
			conn.Write(message)
		}()
	}
	wg_server.Wait()

	var client_message [][]byte
	var mutex sync.Mutex

	var wg sync.WaitGroup
	wg.Add(len(scs.Servers) - 1)
	for i := 0; i < len(scs.Servers)-1; i++ {
		go func() {
			defer wg.Done()
			conn, err := listener.Accept()
			checkError(err)
			defer conn.Close()

			var message_receive [][]byte
			bytes_read := make([]byte, 101)
			for {
				n, err := conn.Read(bytes_read)
				checkError(err)
				if bytes_read[0] == byte(1) {
					break
				}
				message_receive = append(message_receive, bytes_read[:n])
			}
			mutex.Lock()
			client_message = append(client_message, message_receive...)
			mutex.Unlock()
		}()
	}
	wg.Wait()

	//process the message we got
	sortTheMessages(client_message, outputfile)

}

func checkError(err error) {
	if err != nil {
		log.Fatalf("Fatal error: %s", err.Error())
	}
}

// func listenForClientConnections(write_only chan<- []byte, listener net.Listener, scs ServerConfigs, inputfile string) {

// 	defer listener.Close()

// 	for {
// 		conn, err := listener.Accept()
// 		if err != nil {
// 			continue
// 		}
// 		fmt.Println("Client: " + conn.RemoteAddr().String() + " connected, spawning goroutine to handle connection")
// 		// Spawn a new goroutine to handle this client's connections
// 		// and go back to listening for additional connections
// 		go handleClientConnection(conn, write_only, inputfile, scs)

// 	}

// }

// func handleClientConnection(conn net.Conn, write_only_ch chan<- []byte, inputfile string, scs ServerConfigs) {
// 	defer conn.Close()
// 	fmt.Println("Step into handleClientConnection func _ 1")
// 	//get first bits to confirm which data would be sent to this server(client).
// 	server_number := len(scs.Servers)
// 	fmt.Printf("server_number: %d \n", server_number)

// 	bytes_for_server := PowerOfTwo(server_number)
// 	fmt.Printf("bytes_for_server: %d \n", bytes_for_server)

// 	// get the specific client_id by port number
// 	client_socket_string := conn.RemoteAddr().String()
// 	client_port := strings.Split(client_socket_string, ":")[1]
// 	fmt.Printf("Client port is %s \n", client_port)
// 	var client_id int
// 	for i := 0; i < server_number; i++ {
// 		if client_port == scs.Servers[i].Port {
// 			client_id = scs.Servers[i].ServerId
// 		}
// 	}

// 	f, err := os.Open(inputfile)
// 	checkError(err)
// 	defer f.Close()

// 	for {
// 		// read key-value pair from the file
// 		client_msg_buf := make([]byte, 100)
// 		bytes_read, err := f.Read(client_msg_buf)
// 		if err != nil && err != io.EOF {
// 			log.Fatal(err)
// 		}

// 		if err == io.EOF {
// 			break
// 		}
// 		/*
// 			get the first "bytes_for_server" bytes and if it matches the current client_id,
// 			write to the channel
// 		*/
// 		client_number_byte := make([]byte, bytes_for_server)
// 		copy(client_number_byte, client_msg_buf[:bytes_for_server])
// 		client_number := int(binary.BigEndian.Uint32(client_number_byte))
// 		fmt.Println("Bug_6")
// 		fmt.Printf("Client number is : %d \n", client_number)
// 		if client_number == client_id {
// 			fmt.Printf("Writing key-value to %d channel \n", client_id)
// 			stream_complete := []byte{0}
// 			client_msg := client_msg_buf[0:bytes_read]
// 			message := append(stream_complete, client_msg...)
// 			write_only_ch <- message
// 		}

// 	}
// 	write_only_ch <- []byte{1}
// }

// func clientRequestConnection(read_only_ch <-chan []byte, scs ServerConfigs, serverId int) [][]byte {
// 	var message_receive [][]byte
// 	for i := serverId + 1; i < len(scs.Servers); i++ {
// 		server_addr := scs.Servers[i].Host + ":" + scs.Servers[i].Port
// 		fmt.Printf("Client %d ready to request to connect Server %d \n", serverId, scs.Servers[i].ServerId)
// 		var conn net.Conn
// 		for {
// 			conn_t, err := net.Dial("tcp", server_addr)

// 			if err == nil {
// 				fmt.Printf("Client %d connected Server %d Successfully \n", scs.Servers[i].ServerId, serverId)
// 				break
// 			}
// 			conn = conn_t
// 		}
// 		defer conn.Close()

// 		var client_message [][]byte
// 		for {
// 			message := <-read_only_ch
// 			if message[0] == byte(1) {
// 				break
// 			}
// 			client_message = append(client_message, message)
// 		}

// 		message_receive = append(message_receive, client_message...)

// 		// if err != nil {
// 		// 	log.Fatalln("Client" + strconv.Itoa(serverId) + " connected to Server" + server_addr + "Failed")
// 		// }
// 		// only when we get the client_message from all the server, can we exit the main func
// 		// else we need to wait for the messages.
// 		// client_message = append(client_message, receiveDataFromServers(bidirectional_ch)...)

// 	}
// 	return message_receive
// }

// func receiveDataFromServers(read_only_ch <-chan []byte) [][]byte {
// 	/*
// 	 * the main goroutine waits for the completion of all other goroutines it had spawned
// 	 * before exiting.
// 	 */

// 	//only message from specific server do we handle
// 	var client_message [][]byte
// 	for {
// 		message := <-read_only_ch
// 		if message[0] == byte(1) {
// 			break
// 		}
// 		client_message = append(client_message, message)
// 	}
// 	return client_message
// }

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

func sortTheMessages(message [][]byte, outputfile string) {

	sort.Slice(message, func(i, j int) bool { return bytes.Compare(message[i][:11], message[j][:11]) < 0 })

	f, err := os.Create(outputfile)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	for _, element := range message {
		f.Write(element)
	}
}

func extractBits(buf []byte, numBits int) int {
	// Get the byte that contains the bits
	byteNum := numBits / 8
	// Get the position of the bits in the byte
	bitPos := numBits % 8

	// Create a mask to extract the bits
	mask := byte(1<<uint(bitPos)) - 1
	// Shift the byte to the right to discard the unneeded bits
	result := int(buf[byteNum]) & int(mask)

	return result
}
