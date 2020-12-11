package mydynamo

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue         int          //Number of nodes to write to on each Put
	rValue         int          //Number of nodes to read from on each Get
	preferenceList []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode       DynamoNode   //This node's address and port info
	nodeID         string       //ID of this node
	localStore     map[string][]PutArgs
	gossipList     map[string][]PutArgs
	crashed        bool
}

func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	s.preferenceList = incomingList
	return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
	// panic("todo")
	log.Println(len(s.preferenceList))
	for i := 0; i < len(s.preferenceList); i++ {
		if s.preferenceList[i].Port != s.selfNode.Port {

			for j := 0; j < len(s.localStore[s.preferenceList[i].Port]); j++ {
				if _, ok := s.gossipList[s.preferenceList[i].Port]; !ok {
					conn, e := rpc.DialHTTP("tcp", s.preferenceList[i].Address+":"+s.preferenceList[i].Port)
					if e != nil {
						return e
					}
					result := new(bool)
					e = conn.Call("MyDynamo.GossipPut", s.localStore[s.preferenceList[i].Port][j], result)
					if e != nil {
						conn.Close()
						return e
					}
					defer conn.Close()
				}
			}

		}
	}
	return nil
}

//Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
	// panic("todo")
	// *success = false
	if s.crashed {
		return errors.New("Server " + s.selfNode.Port + " is already crashed.")
	}
	s.crashed = true
	go s.ServerCrash(seconds, success)
	s.crashed = false
	// time.Sleep(time.Duration(seconds) * time.Second)
	// *success = true

	// go func(sec int) {
	// 	s.crashed = true
	// 	time.Sleep(time.Duration(seconds) * time.Second)
	// 	s.crashed = false
	// }(seconds)
	return nil
}

func (s *DynamoServer) ServerCrash(seconds int, success *bool) error {
	// panic("todo")

	time.Sleep(time.Duration(seconds) * time.Second)
	// fmt.Print("server is crashed")
	return errors.New("server is crashed")
}

// Put a file to this server and W other servers
func (s *DynamoServer) Put(value PutArgs, result *bool) error {
	// panic("todo")
	log.Println(len(s.preferenceList))
	var m sync.Mutex
	if !s.crashed {
		fmt.Println("Put...", s.localStore)
		if _, ok := s.localStore[value.Key]; !ok {
			// fmt.Println("Put not exit, putting...", s.nodeID, value)
			s.localStore[value.Key] = []PutArgs{value}
			// s.localStore[value.Key][0] = value
			s.localStore[value.Key][0].Context.Clock.Clock[value.Key]++
		} else if s.localStore[value.Key][0].Context.Clock.LessThan(value.Context.Clock) {
			// fmt.Println("Put lessthan, putting...", s.nodeID, value.Value, value.Context.Clock, s.localStore[value.Key][0].Context.Clock)
			s.localStore[value.Key] = []PutArgs{value}
			s.localStore[value.Key][0].Context.Clock.Clock[value.Key]++

		} else if value.Context.Clock.LessThan(s.localStore[value.Key][0].Context.Clock) {
			// fmt.Println("Put not desendent...", s.nodeID, value)
			*result = false
		} else if s.localStore[value.Key][0].Context.Clock.Concurrent(value.Context.Clock) {
			// fmt.Println("Put exit, putting...", s.nodeID, value)
			m.Lock()
			s.localStore[value.Key] = append(s.localStore[value.Key], value)
			s.localStore[value.Key][len(s.localStore[value.Key])-1].Context.Clock.Clock[value.Key]++
			m.Unlock()
		}
	} else {
		return errors.New("Server " + s.selfNode.Port + " is crashed...")
	}
	fmt.Println("Preference list...", s.nodeID, s.selfNode.Port, s.preferenceList)
	// fmt.Println("exist, putting...", value.Key)
	// s.localStore[value.Key] = value

	wNode := s.wValue - 1
	for i := 0; i < wNode; i++ {
		fmt.Println("Put rpc...", i, s.preferenceList[i].Port)
		if s.preferenceList[i].Port != s.selfNode.Port {
			conn, e := rpc.DialHTTP("tcp", s.preferenceList[i].Address+":"+s.preferenceList[i].Port)
			if e != nil {
				return e
			}
			e = conn.Call("MyDynamo.ServerPut", value, result)
			if e != nil {
				conn.Close()
				return e
			}
			m.Lock()
			s.gossipList[s.preferenceList[i].Port] = append(s.gossipList[s.preferenceList[i].Port], value)
			m.Unlock()
			defer conn.Close()
		} else {
			wNode++
		}

		// perform the call
		// rpcClient := RPCClient{
		// 	ServerAddr: s.preferenceList[i].Address,
		// 	rpcConn:    conn,
		// }
		// rpcClient.Put(value)

	}
	*result = true

	return nil
}

// server to server Put
func (s *DynamoServer) ServerPut(value PutArgs, result *bool) error {
	log.Println(len(s.preferenceList))
	var m sync.Mutex
	if !s.crashed {
		fmt.Println("ServerPut...", s.localStore)
		if _, ok := s.localStore[value.Key]; !ok {
			// fmt.Println("ServerPut not exit, putting...", s.nodeID, value)
			s.localStore[value.Key] = []PutArgs{value}
			s.localStore[value.Key][0].Context.Clock.Clock[value.Key]++
		} else if s.localStore[value.Key][0].Context.Clock.LessThan(value.Context.Clock) {
			// fmt.Println("ServerPut lessthan, putting...", s.nodeID, value.Value, value.Context.Clock, s.localStore[value.Key][0].Context.Clock)
			s.localStore[value.Key] = []PutArgs{value}
			// s.localStore[value.Key][0] = value
			s.localStore[value.Key][0].Context.Clock.Clock[value.Key]++

		} else if value.Context.Clock.LessThan(s.localStore[value.Key][0].Context.Clock) {
			fmt.Println("ServerPut not desendent...", s.nodeID, value)
			*result = false
		} else if s.localStore[value.Key][0].Context.Clock.Concurrent(value.Context.Clock) {
			fmt.Println("ServerPut exit, putting...", s.nodeID, value)
			m.Lock()
			s.localStore[value.Key] = append(s.localStore[value.Key], value)
			s.localStore[value.Key][len(s.localStore[value.Key])-1].Context.Clock.Clock[value.Key]++
			m.Unlock()
		}
	} else {
		return errors.New("Server " + s.selfNode.Port + " is crashed...")
	}

	return nil
}

func (s *DynamoServer) GossipPut(value PutArgs, result *bool) error {
	log.Println(len(s.preferenceList))
	var m sync.Mutex
	if !s.crashed {
		fmt.Println("GossipPut...", s.localStore)
		if _, ok := s.localStore[value.Key]; !ok {
			// fmt.Println("GossipPut not exit, putting...", s.nodeID, value)
			s.localStore[value.Key] = []PutArgs{value}
			// s.localStore[value.Key][0].Context.Clock.Clock[value.Key]++
		} else {
			// fmt.Println("GossipPut exit, putting...", s.nodeID, value)
			m.Lock()
			s.localStore[value.Key] = append(s.localStore[value.Key], value)
			// s.localStore[value.Key][len(s.localStore[value.Key])-1].Context.Clock.Clock[value.Key]++
			m.Unlock()
		}
	} else {
		return errors.New("Server " + s.selfNode.Port + " is crashed...")
	}

	return nil
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	// panic("todo")
	var m sync.Mutex
	if !s.crashed {
		if _, ok := s.localStore[key]; ok {
			for i := 0; i < len(s.localStore[key]); i++ {
				fmt.Println("Get ok...", s.localStore[key][i].Value)
				objectEntry := ObjectEntry{
					Context: s.localStore[key][i].Context,
					Value:   s.localStore[key][i].Value,
				}
				m.Lock()
				result.EntryList = append(result.EntryList, objectEntry)
				m.Unlock()
			}

		}
	} else {
		return errors.New("Server " + s.selfNode.Port + " is crashed...")
	}

	rNode := s.rValue - 1
	for i := 0; i < rNode; i++ {
		if s.selfNode.Port != s.preferenceList[i].Port {
			conn, e := rpc.DialHTTP("tcp", s.preferenceList[i].Address+":"+s.preferenceList[i].Port)
			if e != nil {
				return e
			}

			// perform the call
			e = conn.Call("MyDynamo.ServerGet", key, result)
			if e != nil {
				conn.Close()
				return e
			}
			defer conn.Close()
		} else {
			rNode++
		}
	}
	return nil
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) ServerGet(key string, result *DynamoResult) error {
	var m sync.Mutex
	if !s.crashed {
		if _, ok := s.localStore[key]; ok {
			for i := 0; i < len(s.localStore[key]); i++ {
				fmt.Println("ServerGet ok...", s.localStore[key][i].Value)
				objectEntry := ObjectEntry{
					Context: s.localStore[key][i].Context,
					Value:   s.localStore[key][i].Value,
				}
				m.Lock()
				result.EntryList = append(result.EntryList, objectEntry)
				m.Unlock()
			}
		}
	} else {
		return errors.New("Server " + s.selfNode.Port + " is crashed...")
	}

	return nil
}

/* Belows are functions that implement server boot up and initialization */
func NewDynamoServer(w int, r int, hostAddr string, hostPort string, id string) DynamoServer {
	preferenceList := make([]DynamoNode, 0)
	selfNodeInfo := DynamoNode{
		Address: hostAddr,
		Port:    hostPort,
	}
	return DynamoServer{
		wValue:         w,
		rValue:         r,
		preferenceList: preferenceList,
		selfNode:       selfNodeInfo,
		nodeID:         id,
		localStore:     map[string][]PutArgs{},
		gossipList:     map[string][]PutArgs{},
		crashed:        false,
	}
}

func ServeDynamoServer(dynamoServer DynamoServer) error {
	rpcServer := rpc.NewServer()
	e := rpcServer.RegisterName("MyDynamo", &dynamoServer)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Name Registration")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Registered the RPC Interfaces")

	l, e := net.Listen("tcp", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	if e != nil {
		log.Println(DYNAMO_SERVER, "Server Can't start During Port Listening")
		return e
	}

	log.Println(DYNAMO_SERVER, "Successfully Listening to Target Port ", dynamoServer.selfNode.Address+":"+dynamoServer.selfNode.Port)
	log.Println(DYNAMO_SERVER, "Serving Server Now")

	return http.Serve(l, rpcServer)
}
