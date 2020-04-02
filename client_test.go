package bloomd

import (
	"fmt"
	"testing"
)

// Clear everything out of bloomd before running tests.
func TestDropEverything(t *testing.T) {
	client := NewClient(serverAddress)
	filters, _ := client.ListFilters()
	for f := range filters {
		filter := Filter{Name: f, Conn: client.Conn}
		_ = filter.Drop()
	}
}

func TestCreateFilter(t *testing.T) {
	client := NewClient(serverAddress)

	err := client.CreateFilter(&validFilter)
	failIfError(t, err)

	err = client.CreateFilter(&anotherFilter)
	failIfError(t, err)
}

func TestGetFilter(t *testing.T) {
	client := NewClient(serverAddress)

	filter, err := client.GetFilter(validFilter.Name)
	failIfError(t, err)

	if filter.Name != validFilter.Name {
		t.Error("Name not equal")
	}
	if filter.HashKeys != validFilter.HashKeys {
		t.Error("HashKeys not equal")
	}
}

func TestListFilters(t *testing.T) {
	client := NewClient(serverAddress)

	filters, err := client.ListFilters()
	failIfError(t, err)

	if filters[validFilter.Name] == "" {
		fmt.Printf("%+v\n", filters)
		t.Error(validFilter.Name)
	}
}

func TestClientFlush(t *testing.T) {
	client := NewClient(serverAddress)

	err := client.Flush()
	failIfError(t, err)
}
