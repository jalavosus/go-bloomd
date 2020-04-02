// Provides an interface to a single Bloomd filter

package bloomd

import (
	"crypto/sha1"
	"fmt"
	"strings"
)

type Filter struct {
	Name     string
	Conn     *Connection
	HashKeys bool
	// Optional
	Capacity int     // The initial capacity of the filter
	Prob     float64 // The inital probability of false positives
	InMemory bool    // If True, specified that the filter should be created
}

// Returns the key we should send to the server
func (f *Filter) getKey(key string) string {
	if f.HashKeys {
		h := sha1.New()
		s := h.Sum([]byte(key))
		return fmt.Sprintf("%x", s)
	}
	return key
}

// Adds a new key to the filter. Returns True/False if the key was added
func (f *Filter) Set(key string) (bool, error) {
	cmd := fmt.Sprintf("%s %s %s", FilterSetCmd, f.Name, f.getKey(key))

	resp, err := f.Conn.SendAndReceive(cmd)
	if err != nil {
		return false, err
	}

	if resp == FilterYes || resp == FilterNo {
		return resp == FilterYes, nil
	}

	return false, errInvalidResponse(resp)
}

func (f *Filter) groupCommand(kind string, keys []string) (rs []bool, e error) {
	cmd := fmt.Sprintf("%s %s", kind, f.Name)

	for _, key := range keys {
		cmd = fmt.Sprintf("%s %s", cmd, f.getKey(key))
	}

	resp, e := f.Conn.SendAndReceive(cmd)
	if e != nil {
		return rs, &BloomdError{ErrorString: e.Error()}
	}

	if strings.HasPrefix(resp, FilterYes) || strings.HasPrefix(resp, FilterNo) {
		split := strings.Split(resp, " ")
		for _, res := range split {
			rs = append(rs, res == FilterYes)
		}
	}
	return rs, nil
}

func (f *Filter) singleCommand(kind string, key string) (rs bool, err error) {
	cmd := fmt.Sprintf("%s %s %s", kind, f.Name, f.getKey(key))

	resp, err := f.Conn.SendAndReceive(cmd)
	if err != nil {
		return rs, &BloomdError{ErrorString: err.Error()}
	}

	if strings.HasPrefix(resp, FilterYes) || strings.HasPrefix(resp, FilterNo) {
		rs = resp == FilterYes
	}

	return rs, nil
}

// Performs a bulk set command, adds multiple keys in the filter
func (f *Filter) Bulk(keys []string) (responses []bool, err error) {
	return f.groupCommand(FilterBulkCmd, keys)
}

func (f *Filter) Check(key string) (response bool, err error) {
	return f.singleCommand(FilterCheckCmd, key)
}

// Performs a multi command, checks for multiple keys in the filter
func (f *Filter) Multi(keys []string) (responses []bool, err error) {
	return f.groupCommand(FilterMultiCmd, keys)
}

func (f *Filter) sendCommand(cmd string) error {
	sendCmd := fmt.Sprintf("%s %s", cmd, f.Name)

	resp, err := f.Conn.SendAndReceive(sendCmd)
	if err != nil {
		return err
	}

	if resp != RespDone {
		return errInvalidResponse(resp)
	}

	return nil
}

// Deletes the filter permanently from the server
func (f *Filter) Drop() error {
	return f.sendCommand(FilterDropCmd)
}

// Closes the filter on the server
func (f *Filter) Close() error {
	return f.sendCommand(FilterCloseCmd)
}

// Clears the filter on the server
func (f *Filter) Clear() error {
	return f.sendCommand(FilterClearCmd)
}

// Forces the filter to flush to disk
func (f *Filter) Flush() error {
	return f.sendCommand(FlushCmd)
}

// Returns the info dictionary about the filter
func (f *Filter) Info() (map[string]string, error) {
	sendCmd := fmt.Sprintf("%s %s", InfoCmd, f.Name)

	if err := f.Conn.Send(sendCmd); err != nil {
		return nil, err
	}

	info, err := f.Conn.responseBlockToMap()
	if err != nil {
		return nil, err
	}

	return info, nil
}
