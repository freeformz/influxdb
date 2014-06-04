package coordinator

import (
	"encoding/binary"
	"net"
	"protocol"
	"sync"
	"sync/atomic"
	"time"

	log "code.google.com/p/log4go"
)

type ProtobufClient struct {
	conn          net.Conn
	hostAndPort   string
	reconnectWait sync.WaitGroup
	connectCalled bool
	lastRequestId uint32
	writeTimeout  time.Duration
	attempts      int
	stopped       bool
	requests      chan wrappedRequest
	responses     chan *protocol.Response
	clear         chan struct{}
}

type runningRequest struct {
	timeMade     time.Time
	responseChan chan *protocol.Response
	request      *protocol.Request
}

const (
	REQUEST_RETRY_ATTEMPTS = 2
	MAX_RESPONSE_SIZE      = MAX_REQUEST_SIZE
	MAX_REQUEST_TIME       = time.Second * 1200
	RECONNECT_RETRY_WAIT   = time.Millisecond * 100
)

func NewProtobufClient(hostAndPort string, writeTimeout time.Duration) *ProtobufClient {
	log.Debug("NewProtobufClient: ", hostAndPort)
	return &ProtobufClient{
		hostAndPort:  hostAndPort,
		writeTimeout: writeTimeout,
		stopped:      false,
		requests:     make(chan wrappedRequest),
		responses:    make(chan *protocol.Response),
		clear:        make(chan struct{}),
	}
}

type wrappedRequest struct {
	id           uint32
	data         []byte
	timeMade     time.Time
	responseChan chan *protocol.Response
}

func (self *ProtobufClient) RequestResponseLoop() {
	futures := make(map[uint32]wrappedRequest)
	timeout := time.NewTicker(time.Minute)

	for !self.stopped {
		select {
		case response := <-self.responses:
			req, ok := futures[*response.RequestId]
			if ok {
				delete(futures, *response.RequestId)
				req.responseChan <- response
			}

		case request := <-self.requests:
			if self.conn == nil {
				self.reconnect()
			}

			if self.writeTimeout > 0 {
				self.conn.SetWriteDeadline(time.Now().Add(self.writeTimeout))
			}

			if request.responseChan != nil {
				request.timeMade = time.Now()
				futures[request.id] = request
			}

			binary.Write(self.conn, binary.LittleEndian, uint32(len(request.data)))
			self.conn.Write(request.data)

		case <-self.clear:
			message := "clearing all requests"
			for k, req := range futures {
				select {
				case req.responseChan <- &protocol.Response{Type: &endStreamResponse, ErrorMessage: &message}:
				default:
					log.Debug("Cannot send response on channel")
				}
				delete(futures, k)
			}

		case when := <-timeout.C:
			maxAge := when.Add(-MAX_REQUEST_TIME)
			for k, req := range futures {
				if req.timeMade.Before(maxAge) {
					delete(futures, k)
					log.Warn("Request timed out: ", req.id)
				}
			}
		}
	}
}

func (self *ProtobufClient) Connect() {
	if self.connectCalled {
		return
	}
	self.connectCalled = true
	go func() {
		self.RequestResponseLoop()
		self.readResponses()
	}()
}

func (self *ProtobufClient) Close() {
	if self.conn != nil {
		self.conn.Close()
		self.stopped = true
		self.conn = nil
	}
	self.ClearRequests()
}

func (self *ProtobufClient) ClearRequests() {
	self.clear <- struct{}{}
}

// Makes a request to the server. If the responseStream chan is not nil it will expect a response from the server
// with a matching request.Id. The REQUEST_RETRY_ATTEMPTS constant of 3 and the RECONNECT_RETRY_WAIT of 100ms means
// that an attempt to make a request to a downed server will take 300ms to time out.
func (self *ProtobufClient) MakeRequest(request *protocol.Request, responseStream chan *protocol.Response) error {

	if request.Id == nil {
		id := atomic.AddUint32(&self.lastRequestId, uint32(1))
		request.Id = &id
	}

	data, err := request.Encode()
	if err != nil {
		return err
	}

	self.requests <- wrappedRequest{id: *request.Id, data: data, responseChan: responseStream}
	return nil
}

func (self *ProtobufClient) readResponses() {
	for !self.stopped {
		if self.conn == nil {
			time.Sleep(200 * time.Millisecond)
			continue
		}

		var messageSizeU uint32
		var err error
		err = binary.Read(self.conn, binary.LittleEndian, &messageSizeU)
		if err != nil {
			log.Error("Error while reading messsage size: %d", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		buff := make([]byte, int(messageSizeU))
		_, err = self.conn.Read(buff)
		if err != nil {
			log.Error("Error while reading message: %d", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}

		response, err := protocol.DecodeResponse(buff)
		if err != nil {
			log.Error("error unmarshaling response: %s", err)
			time.Sleep(200 * time.Millisecond)
			continue
		}
		self.responses <- response
	}
}

func (self *ProtobufClient) reconnect() net.Conn {
	if self.conn != nil {
		self.conn.Close()
	}
	for attempts := 0; attempts < 100; attempts++ {
		conn, err := net.DialTimeout("tcp", self.hostAndPort, self.writeTimeout)
		if err == nil {
			self.conn = conn
			log.Info("connected to %s", self.hostAndPort)
			return self.conn
		}
		time.Sleep(RECONNECT_RETRY_WAIT)
	}
	log.Error("Unable to connect to server after 100 attempts: %s", self.hostAndPort)
	return nil
}
