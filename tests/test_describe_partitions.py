package main

import (
	"fmt"
	"net"
	"os"
	"encoding/binary"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func accept(l net.Listener) {
	conn, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	go serviceConnection(conn);
}

func encodeLength(l uint16) []byte {
	out := make([]byte, 0);
	if l == 0 {
		out = append(out, 0);
	} else {
		for {
			next := byte(l & 0x3f);
			l = l >> 7;
			if l == 0 {
				out = append(out, next);
				break;
			} else {
				next = next | 0x80;
				out = append(out, next);
			}
		}
	}

	return out;
}

func serviceConnection(conn net.Conn) {
	defer conn.Close();

	for {
		incoming := make([]byte, 1024);
		_, err := conn.Read(incoming);
		if err != nil {
			fmt.Println("Error reading: ", err.Error())
			return;
		}

		apiKey := binary.BigEndian.Uint16(incoming[4:]);
		apiVersion := binary.BigEndian.Uint16(incoming[6:]);
		correlationID := binary.BigEndian.Uint32(incoming[8:]);
		//softwareNameLen := binary.BigEndian.Uint16(incoming[12:]);
		//softwareVersionLen := binary.BigEndian.Uint16(incoming[14+softwareNameLen:]);

		if apiVersion == 4 {
			arr := make([]byte, 30);
			binary.BigEndian.PutUint32(arr, uint32(len(arr)) - 4);
			binary.BigEndian.PutUint32(arr[4:], correlationID);
			binary.BigEndian.PutUint16(arr[8:], 0);
			arr[10] = 0x3;
			binary.BigEndian.PutUint16(arr[11:], apiKey);
			binary.BigEndian.PutUint16(arr[13:], 4); // min version
			binary.BigEndian.PutUint16(arr[15:], 4); // max version
			arr[17] = 0;
			binary.BigEndian.PutUint16(arr[18:], 75);
			binary.BigEndian.PutUint16(arr[20:], 0); // min version
			binary.BigEndian.PutUint16(arr[22:], 0); // max version
			arr[24] = 0;
			binary.BigEndian.PutUint32(arr[25:], 0);
			arr[29] = 0;
			
			if _, err = conn.Write(arr); err != nil {
				fmt.Println("Error writing: ", err.Error())
			}
		} else if apiVersion == 0 {
			topicsLen := binary.BigEndian.Uint16(incoming[12:]);
			topicsEnd := 14+topicsLen;
			topics := string(incoming[14:topicsEnd]);

			topics2Len := uint16(incoming[topicsEnd+2]-1);
			topics2 := string(incoming[topicsEnd+3:topicsEnd+3+topics2Len]);
			fmt.Println("T2", topics2Len, topics2);

			arr := make([]byte, 4);
			arr = binary.BigEndian.AppendUint32(arr, correlationID);
			arr = append(arr, 0); // tag buffer
			arr = binary.BigEndian.AppendUint32(arr, 0); // throttle

			arr = append(arr, 2);
			arr = binary.BigEndian.AppendUint16(arr, 3);
			arr = append(arr, encodeLength(topics2Len+1)...);
			arr = append(arr, topics2...);
			arr = binary.BigEndian.AppendUint64(arr, 0);
			arr = binary.BigEndian.AppendUint64(arr, 0);
			arr = append(arr, 0); // is internal
			arr = binary.BigEndian.AppendUint16(arr, 0); // partition count
			arr = append(arr, 0); // tag buffer
			arr = binary.BigEndian.AppendUint32(arr, 0); // topic_authorized_operations

			// next cursor
			arr = append(arr, encodeLength(topicsLen+1)...);
			arr = append(arr, topics...);
			arr = binary.BigEndian.AppendUint32(arr, 0); // partition index
			arr = append(arr, 0); // tag buffer
			arr = append(arr, 0); // tag buffer

			binary.BigEndian.PutUint32(arr, uint32(len(arr)) - 4);
			if _, err = conn.Write(arr); err != nil {
				fmt.Println("Error writing: ", err.Error())
			}
		} else {
			arr := make([]byte, 10);
			binary.BigEndian.PutUint32(arr, 0);
			binary.BigEndian.PutUint32(arr[4:], correlationID);
			binary.BigEndian.PutUint16(arr[8:], 35);
			if _, err = conn.Write(arr); err != nil {
				fmt.Println("Error writing: ", err.Error())
			}
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	for {
		accept(l);
	}
}