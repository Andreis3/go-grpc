package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/Andreis3/go-grpc/pb"
	"google.golang.org/grpc"
)

func main() {
	connection, err := grpc.Dial("localhost:50051", grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Could not connect to grpc Server: %v", err)
	}

	defer connection.Close()

	client := pb.NewUserServiceClient(connection)

	//AddUser(client)
	// AddUserVerbose(client)
	// AddUsers(client)
	AddUserStreamBoth(client)
}

func AddUser(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "0",
		Name:  "Andrei",
		Email: "a@a.com",
	}

	res, err := client.AddUser(context.Background(), req)

	if err != nil {
		log.Fatalf("Could not make to grpc request: %v", err)
	}

	fmt.Println(res)
}

func AddUserVerbose(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "0",
		Name:  "Andrei",
		Email: "a@a.com",
	}

	responseStream, err := client.AddUserVerbose(context.Background(), req)

	if err != nil {
		log.Fatalf("Could not make to grpc request: %v", err)
	}

	for {
		stream, err := responseStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Could not receive the msg: %v", err)
		}
		fmt.Println("Status:", stream.Status, " - ", stream.GetUser())
	}
}

func AddUsers(client pb.UserServiceClient) {
	reqs := []*pb.User{
		&pb.User{
			Id:    "w1",
			Name:  "Andrei 1",
			Email: "andrei-1@rei.com",
		},
		&pb.User{
			Id:    "w2",
			Name:  "Andrei 2",
			Email: "andrei-2@rei.com",
		},
		&pb.User{
			Id:    "w3",
			Name:  "Andrei 3",
			Email: "andrei-3@rei.com",
		},
		&pb.User{
			Id:    "w4",
			Name:  "Andrei 4",
			Email: "andrei-4@rei.com",
		},
		&pb.User{
			Id:    "w5",
			Name:  "Andrei 5",
			Email: "andrei-5@rei.com",
		},
		&pb.User{
			Id:    "w6",
			Name:  "Andrei 6",
			Email: "andrei-6@rei.com",
		},
	}

	stream, err := client.AddUsers(context.Background())
	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	for _, req := range reqs {
		stream.Send(req)
		time.Sleep(time.Second * 3)
	}

	res, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error receiving response: %v", err)
	}

	fmt.Println(res)

}

func AddUserStreamBoth(client pb.UserServiceClient) {
	stream, err := client.AddUserStreamBoth(context.Background())
	if err != nil {
		log.Fatalf("Could not make to grpc request: %v", err)
	}

	reqs := []*pb.User{
		&pb.User{
			Id:    "w1",
			Name:  "Andrei 1",
			Email: "andrei-1@rei.com",
		},
		&pb.User{
			Id:    "w2",
			Name:  "Andrei 2",
			Email: "andrei-2@rei.com",
		},
		&pb.User{
			Id:    "w3",
			Name:  "Andrei 3",
			Email: "andrei-3@rei.com",
		},
		&pb.User{
			Id:    "w4",
			Name:  "Andrei 4",
			Email: "andrei-4@rei.com",
		},
		&pb.User{
			Id:    "w5",
			Name:  "Andrei 5",
			Email: "andrei-5@rei.com",
		},
		&pb.User{
			Id:    "w6",
			Name:  "Andrei 6",
			Email: "andrei-6@rei.com",
		},
	}

	go func() {
		for _, req := range reqs {
			fmt.Println("Sending user:", req.Name)
			stream.Send(req)
			time.Sleep(time.Second * 2)
		}
		stream.CloseSend()
	}()

	wait := make(chan int)

	go func() {
		for {
			res, err := stream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalf("Error receiving data: %v", err)
				break
			}
			fmt.Printf("Receiving user with status: %v", res.GetUser().Name, res.GetStatus())
		}
		close(wait)
	}()

	<-wait
}
