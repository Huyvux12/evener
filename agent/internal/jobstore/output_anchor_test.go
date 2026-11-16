package jobstore

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
)

// A zero-length durable anchor carries no evidence about any byte: the prefix
// check hashes nothing, and the empty-file checksum matches every file. Accepting
// a short log on that basis would report bytes nothing vouched for as job output,
// so a zero anchor must not rescue a log with surviving content.
func TestOutputRejectsAZeroAnchorThatVouchesForNothing(t *testing.T) {
	base := afero.NewMemMapFs()
	const log = "/job.log"
	const meta = "/job.log.meta.json"
	// The sidecar names 40 bytes; the log holds 18 different ones.
	vouched := bytes.Repeat([]byte("a"), 40)
	if err := afero.WriteFile(base, log, bytes.Repeat([]byte("b"), 18), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeOutputMetaFileFsSync(base, meta, outputMeta{
		TotalBytes:      40,
		RetainedSHA256:  outputBytesSHA256(vouched),
		DurableFileSize: new(int64(0)),
		DurableSHA256:   outputBytesSHA256(nil), // the empty-file checksum
	}, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readValidOutputMetaFs(base, meta, log, 18); err == nil {
		t.Fatal("a zero anchor accepted 18 unvouched bytes as output: the prefix check hashes nothing and the empty-file checksum matches any file")
	}
}

// The anchor is what separates a lost tail from a replaced log, so a prefix that
// disagrees with it must be refused even when the log is shorter than its
// metadata. Without this the tolerance accepts every truncation.
func TestOutputRejectsALostTailWhosePrefixDisagreesWithTheAnchor(t *testing.T) {
	base := afero.NewMemMapFs()
	const log = "/job.log"
	const meta = "/job.log.meta.json"
	vouched := bytes.Repeat([]byte("a"), 40)
	if err := afero.WriteFile(base, log, append(bytes.Repeat([]byte("z"), 10), bytes.Repeat([]byte("b"), 8)...), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeOutputMetaFileFsSync(base, meta, outputMeta{
		TotalBytes:      40,
		RetainedSHA256:  outputBytesSHA256(vouched),
		DurableFileSize: new(int64(10)),
		DurableSHA256:   outputBytesSHA256(vouched[:10]),
	}, false); err != nil {
		t.Fatal(err)
	}
	if _, _, err := readValidOutputMetaFs(base, meta, log, 18); err == nil {
		t.Fatal("a log whose first 10 bytes contradict the anchor was accepted as a lost tail")
	}
}

// The tolerance still has to work when the anchor genuinely covers a prefix of
// the surviving bytes: that is the state a power crash leaves.
func TestOutputAcceptsALostTailWhosePrefixMatchesTheAnchor(t *testing.T) {
	base := afero.NewMemMapFs()
	const log = "/job.log"
	const meta = "/job.log.meta.json"
	vouched := bytes.Repeat([]byte("a"), 40)
	if err := afero.WriteFile(base, log, vouched[:18], 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeOutputMetaFileFsSync(base, meta, outputMeta{
		TotalBytes:      40,
		RetainedSHA256:  outputBytesSHA256(vouched),
		DurableFileSize: new(int64(10)),
		DurableSHA256:   outputBytesSHA256(vouched[:10]),
	}, false); err != nil {
		t.Fatal(err)
	}
	got, ok, err := readValidOutputMetaFs(base, meta, log, 18)
	if err != nil || !ok {
		t.Fatalf("lost tail with a matching anchor: ok=%v err=%v", ok, err)
	}
	if got.TotalBytes != 18 {
		t.Fatalf("total = %d, want the surviving 18", got.TotalBytes)
	}
}

var _ = filepath.Join
