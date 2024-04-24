package network

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type NetworkTrafficHandler func(data []byte)

func getNetworkAddress(ip string, port int32) string {
	return fmt.Sprintf("%s:%d", ip, port)
}

func TcpSend(ip string, port int32, data []byte) error {
	var network_address string = getNetworkAddress(ip, port)
	connection, err := net.Dial("tcp", network_address)
	if err != nil {
		return err
	}

	defer connection.Close()

	_, err = connection.Write(data)
	return err
}

func TcpListen(ip string, port int32, handler NetworkTrafficHandler) {
	var network_address string = getNetworkAddress(ip, port)

	go func(handler NetworkTrafficHandler) {

		listener, err := net.Listen("tcp", network_address)
		if err != nil {
			fmt.Println(err)
			return
		}

		defer listener.Close()

		for {
			func() {
				connection, err := listener.Accept()
				if err != nil {
					return
				}

				defer connection.Close()

				reader := bufio.NewReader(connection)

				data_size := make([]byte, 4)
				_, err = io.ReadFull(reader, data_size)
				if err != nil {
					return
				}

				data := make([]byte, binary.BigEndian.Uint32(data_size))
				_, err = io.ReadFull(reader, data)
				if err != nil {
					return
				}

				handler(data)
			}()
		}
	}(handler)
}
