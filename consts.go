package bloomd

// Response constants
const (
	FilterNotFound string = "Filter does not exist"
	RespDone       string = "Done"
	RespExists     string = "Exists"
	FilterYes      string = "Yes"
	FilterNo       string = "No"
)

// Parsing constants
const (
	BlockStart string = "START"
	BlockEnd   string = "END"
	Done       string = "DONE"
)

// Server command constants
const (
	ListCmd   string = "list"
	InfoCmd   string = "info"
	FlushCmd  string = "flush"
	CreateCmd string = "create"
)

// Filter command constants
const (
	FilterSetCmd   string = "set"
	FilterBulkCmd  string = "bulk"
	FilterCheckCmd string = "check"
	FilterMultiCmd string = "multi"
	FilterDropCmd  string = "drop"
	FilterCloseCmd string = "close"
	FilterClearCmd string = "clear"
)
