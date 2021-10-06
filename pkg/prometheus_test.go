package exporter

import (
	"regexp"
	"strings"
	"testing"
	"unicode"

	"github.com/stretchr/testify/assert"
)

var (
	regexGlobal = regexp.MustCompile(`([a-z0-9])([A-Z])`)

	testCases = []struct {
		input  string
		output string
	}{
		{
			input:  "GlobalTopicCount",
			output: "Global.Topic.Count",
		},
		{
			input:  "CPUUtilization",
			output: "CPUUtilization",
		},
		{
			input:  "NetworkIn",
			output: "Network.In",
		},
		{
			input:  "NetworkOut",
			output: "Network.Out",
		},
		{
			input:  "NetworkPacketsIn",
			output: "Network.Packets.In",
		},
		{
			input:  "NetworkPacketsOut",
			output: "Network.Packets.Out",
		},
		{
			input:  "DiskReadBytes",
			output: "Disk.Read.Bytes",
		},
		{
			input:  "DiskWriteBytes",
			output: "Disk.Write.Bytes",
		},
		{
			input:  "DiskReadOps",
			output: "Disk.Read.Ops",
		},
	}
)

func TestSprintString(t *testing.T) {
	for _, tc := range testCases {
		// assert.Equal(t, tc.output, splitString(tc.input))
		assert.Equal(t, tc.output, original(tc.input))
		assert.Equal(t, tc.output, splitStringGlobal(tc.input))
		assert.Equal(t, tc.output, splitStringByHand(tc.input))
	}
}

func original(text string) string {
	splitRegexp := regexp.MustCompile(`([a-z0-9])([A-Z])`)
	return splitRegexp.ReplaceAllString(text, `$1.$2`)
}

func splitStringByHand(text string) string {
	var sb strings.Builder

	var previous rune = 'A'
	for _, r := range text {
		if unicode.IsUpper(r) && (unicode.IsNumber(previous) || unicode.IsLower(previous)) {
			sb.WriteRune('.')
		}
		sb.WriteRune(r)
		previous = r
	}

	return sb.String()
}

func splitStringGlobal(text string) string {
	return regexGlobal.ReplaceAllString(text, `$1.$2`)
}

func BenchmarkOriginal(b *testing.B) {
	for _, tc := range testCases {
		original(tc.input)
	}
}

func BenchmarkGlobalCompile(b *testing.B) {
	for _, tc := range testCases {
		splitStringGlobal(tc.input)
	}
}

func BenchmarkByHand(b *testing.B) {
	for _, tc := range testCases {
		splitStringByHand(tc.input)
	}
}
