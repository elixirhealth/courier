package client

import (
	api "github.com/elixirhealth/courier/pkg/courierapi"
	"google.golang.org/grpc"
)

// NewInsecure returns a new CourierClient without any TLS on the connection.
func NewInsecure(address string) (api.CourierClient, error) {
	cc, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}
	return api.NewCourierClient(cc), nil
}
