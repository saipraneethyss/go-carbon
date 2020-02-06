package stat

// FileStats provides various statistics about file, including Size, RealSize (blocks * 512b), ATime (if available), etc
type FileStats struct {
	Size int64
	// RealSize is size that's occupied by file, including sparse
	RealSize int64
	// If ATime is not available for the platform, it will return 0
	ATime    int64
	ATimeNS  int64

	// If CTime is not available for the platform, it will return MTime
	CTime    int64
	CTimeNS  int64

	MTime    int64
	MTimeNS  int64
}

// MetricUpdate type to convey info about new cache adds and deletes
type MetricOP int

const (
	ADD MetricOP = iota
	DEL
)

type MetricUpdate struct {
	Name         string
	Operation    MetricOP
}
