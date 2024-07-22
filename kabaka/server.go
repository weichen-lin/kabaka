package kabaka

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

type Server struct {
	config *Config
	close  func() error
}

func NewServer(config *Config, close func() error) *Server {
	return &Server{
		config: config,
		close:  close,
	}
}

// Start starts the service.
func (s *Server) Start(ctx context.Context) error {
	addr, err := net.ResolveTCPAddr("tcp", s.config.Addr)
	if err != nil {
		return err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				s.config.logger.Info("server closed by context", ctx.Err())
				return
			default:
				conn, err := l.Accept()
				if err != nil {
					s.config.logger.Error("listen error", err)
					continue
				}

				go func() {
					err := s.handleRequest(conn)
					if err != nil {
						s.config.logger.Error("handle request error", err)
					}
				}()
			}
		}
	}()

	return nil
}

func (s *Server) handleRequest(conn net.Conn) error {
	defer conn.Close()

	for {
		var size int32

		// In kafka protocol, first 4 bytes show the size of the message.
		// So we can use io.Reader to read it.
		// note: 大端序 & 小端序
		// ui practice: https://www.binaryconvert.com/result_signed_int.html?decimal=053048052054050057055054
		// explain: https://studygolang.com/articles/1122
		err := binary.Read(conn, binary.BigEndian, &size)

		// overflow -> 大於 math.MaxInt32
		if size < 0 {
			return fmt.Errorf("size is negative (possibly due to overflow): %d", size)
		}

		// end up with connection close.
		if err == io.EOF {
			s.config.logger.Info("client closed")
			return nil
		}

		fmt.Println("size: ", size)
	}
}
