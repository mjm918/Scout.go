package errors

import "errors"

var (
	ErrNotFound         = errors.New("not found")
	ErrTimeout          = errors.New("timeout")
	ErrNoUpdate         = errors.New("no update")
	ErrNil              = errors.New("data is nil")
	ErrUnsupportedEvent = errors.New("unsupported event")
	ErrBootIndex        = errors.New("error booting index")
	ErrCreateIndex      = errors.New("failed to create index")
	ErrOpenIndex        = errors.New("failed to open index")
	ErrCloseIndex       = errors.New("failed to close index")
	ErrNoDoc            = errors.New("failed to get document")
	ErrFoundDoc         = errors.New("document does not found")
	ErrSearchDoc        = errors.New("failed to search documents")
	ErrIndexDoc         = errors.New("failed to index document")
	ErrDeleteDoc        = errors.New("failed to delete document")
	ErrMissingId        = errors.New("missing id")
	ErrMissingFields    = errors.New("missing fields")
	ErrIndexBatch       = errors.New("failed to batch index")
	ErrCreateConfig     = errors.New("failed create internal.db connection")
)
