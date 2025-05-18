package main

import (
	"testing"
)

func TestReadVarint(t *testing.T) {
	tests := []struct {
		name      string
		input     []byte
		wantValue int64
		wantSize  int64
		expectErr bool
	}{
		{
			name:      "1-byte varint (zero)",
			input:     []byte{0x00},
			wantValue: 0,
			wantSize:  1,
		},
		{
			name:      "1-byte varint (127)",
			input:     []byte{0x7F},
			wantValue: 127,
			wantSize:  1,
		},
		{
			name:      "1-byte varint (42)",
			input:     []byte{0x2A},
			wantValue: 42,
			wantSize:  1,
		},
		{
			name:      "2-byte varint",
			input:     []byte{0x81, 0x01},
			wantValue: 129,
			wantSize:  2,
		},
		{
			name:      "3-byte varint",
			input:     []byte{0x81, 0x81, 0x01},
			wantValue: 16513,
			wantSize:  3,
		},
		{
			name:      "9-byte varint",
			input:     []byte{0xBF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
			wantValue: 0x7FFFFFFFFFFFFFFF,
			wantSize:  9,
		},
		{
			name:      "early termination at byte 8",
			input:     []byte{0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01},
			wantValue: 1,
			wantSize:  8,
		},
		{
			name:      "early termination at byte 4",
			input:     []byte{0x81, 0xBF, 0x81, 0x3F},
			wantValue: 3129535,
			wantSize:  4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			gotValue, gotSize, err := ReadVarint(tt.input)

			if (err != nil) != tt.expectErr {
				t.Errorf("unexpected error: %v", err)
			}
			if gotValue != tt.wantValue {
				t.Errorf("got value = %d, want %d", gotValue, tt.wantValue)
			}
			if gotSize != tt.wantSize {
				t.Errorf("got size = %d, want %d", gotSize, tt.wantSize)
			}
		})
	}
}
