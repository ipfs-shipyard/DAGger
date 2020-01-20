package testhelpers

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"log"

	"github.com/ulikunitz/xz"
	"github.com/ulikunitz/xz/lzma"
)

func EncodeTestVector(data []byte) string {

	var out bytes.Buffer
	b64 := base64.NewEncoder(base64.StdEncoding, &out)

	compressor, initErr := xz.WriterConfig{
		Properties: &(lzma.Properties{
			PB: 4,
			LC: 1,
			LP: 3,
		}),
		DictCap:  32 * 1024 * 1024,
		BufSize:  8192,
		CheckSum: xz.CRC32,
	}.NewWriter(b64)

	if initErr != nil {
		log.Panicf("Failied to initialize XZ compressor: %s", initErr)
	}

	if _, err := compressor.Write(data); err != nil {
		log.Panicf("Unexpected error writing to compressor: %s", err)
	}
	if err := compressor.Close(); err != nil {
		log.Panicf("Unexpected error flushing compressor: %s", err)
	}
	if err := b64.Close(); err != nil {
		log.Panicf("Unexpected error flushing base64 encoder: %s", err)
	}

	return fmt.Sprintf(
		"\nFollows the complete test corpus, decode with: `{some-cli-paste} | base64 --decode | xz -dc | less -S`\n\n%s\n\t",
		out.Bytes(),
	)
}
