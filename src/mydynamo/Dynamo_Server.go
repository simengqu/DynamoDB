package mydynamo

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"time"
)

type DynamoServer struct {
	/*------------Dynamo-specific-------------*/
	wValue         int          //Number of nodes to write to on each Put
	rValue         int          //Number of nodes to read from on each Get
	preferenceList []DynamoNode //Ordered list of other Dynamo nodes to perform operations o
	selfNode       DynamoNode   //This node's address and port info
	nodeID         string       //ID of this node
	localStore     map[string]PutArgs
	gossipList     []string
	crashed        bool
}

func (s *DynamoServer) SendPreferenceList(incomingList []DynamoNode, _ *Empty) error {
	s.preferenceList = incomingList
	return nil
}

// Forces server to gossip
// As this method takes no arguments, we must use the Empty placeholder
func (s *DynamoServer) Gossip(_ Empty, _ *Empty) error {
	panic("todo")
}

//Makes server unavailable for some seconds
func (s *DynamoServer) Crash(seconds int, success *bool) error {
	// panic("todo")
	time.Sleep(time.Duration(seconds) * time.Second)
	*success = true
	return nil
}

// Put a file to this server and W other servers
func (s *DynamoServer) Put(value PutArgs, result *bool) error {
	// panic("todo")
	if !s.crashed {
		if _, ok := s.localStore[value.Key]; !ok {
			fmt.Println("not exit, putting...", value.Key)
			s.localStore[value.Key] = value
		} else if s.localStore[value.Key].Context.Clock.LessThan(value.Context.Clock) {
			fmt.Println("not desendent...", value.Key)
			*result = false
		}
	}

	fmt.Println("exist, putting...", value.Key)
	s.localStore[value.Key] = value
	s.localStore[value.Key].Context.Clock.Clock[value.Key] += 1

	for i := 0; i < s.wValue-1; i++ {
		conn, e := rpc.DialHTTP("tcp", s.preferenceList[i].Address+":"+s.preferenceList[i].Port)
		if e != nil {
			return e
		}

		// perform the call
		// rpcClient := RPCClient{
		// 	ServerAddr: s.preferenceList[i].Address,
		// 	rpcConn:    conn,
		// }
		// rpcClient.Put(value)
		e = conn.Call("MyDynamo.ServerPut", value, result)
		if e != nil {
			conn.Close()
			return e
		}
		defer conn.Close()
	}
	*result = true

	return nil
}

// server to server Put
func (s *DynamoServer) ServerPut(value PutArgs, result *bool) error {
	if !s.crashed {
		if _, ok := s.localStore[value.Key]; !ok {
			fmt.Println("ServerPut not exit, putting...", value.Key)
			s.localStore[value.Key] = value
		} else if s.localStore[value.Key].Context.Clock.LessThan(value.Context.Clock) {
			fmt.Println("ServerPut not desendent...", value.Key)
			*result = false
		}
	}

	return nil
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) Get(key string, result *DynamoResult) error {
	// panic("todo")
	if !s.crashed {
		if _, ok := s.localStore[key]; ok {
			objectEntry := ObjectEntry{
				Context: s.localStore[key].Context,
				Value:   s.localStore[key].Value,
			}
			result.EntryList = append(result.EntryList, objectEntry)
		}
	}

	for i := 0; i < s.rValue-1; i++ {
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
	}
	return nil
}

//Get a file from this server, matched with R other servers
func (s *DynamoServer) ServerGet(key string, result *DynamoResult) error {
	if !s.crashed {
		if _, ok := s.localStore[key]; ok {
			objectEntry := ObjectEntry{
				Context: s.localStore[key].Context,
				Value:   s.localStore[key].Value,
			}
			result.EntryList = append(result.EntryList, objectEntry)
		}
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
		localStore:     map[string]PutArgs{},
		gossipList:     []string{},
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
