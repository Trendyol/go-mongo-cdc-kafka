package helpers

import (
	"testing"
)

func TestChunkSliceWithSize(t *testing.T) {
	tests := []struct {
		name      string
		slice     []int
		chunkSize int
		want      [][]int
	}{
		{
			name:      "empty slice",
			slice:     []int{},
			chunkSize: 2,
			want:      [][]int{},
		},
		{
			name:      "single chunk",
			slice:     []int{1, 2, 3},
			chunkSize: 5,
			want:      [][]int{{1, 2, 3}},
		},
		{
			name:      "exact chunks",
			slice:     []int{1, 2, 3, 4, 5, 6},
			chunkSize: 2,
			want:      [][]int{{1, 2}, {3, 4}, {5, 6}},
		},
		{
			name:      "uneven chunks",
			slice:     []int{1, 2, 3, 4, 5},
			chunkSize: 2,
			want:      [][]int{{1, 2}, {3, 4}, {5}},
		},
		{
			name:      "large chunk size",
			slice:     []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			chunkSize: 3,
			want:      [][]int{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10}},
		},
		{
			name:      "chunk size 1",
			slice:     []int{1, 2, 3},
			chunkSize: 1,
			want:      [][]int{{1}, {2}, {3}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ChunkSliceWithSize(tt.slice, tt.chunkSize)
			if len(got) != len(tt.want) {
				t.Errorf("ChunkSliceWithSize() got %d chunks, want %d chunks", len(got), len(tt.want))
				return
			}
			for i := range got {
				if len(got[i]) != len(tt.want[i]) {
					t.Errorf("ChunkSliceWithSize() chunk %d has %d elements, want %d elements", i, len(got[i]), len(tt.want[i]))
					return
				}
				for j := range got[i] {
					if got[i][j] != tt.want[i][j] {
						t.Errorf("ChunkSliceWithSize() chunk %d, element %d = %v, want %v", i, j, got[i][j], tt.want[i][j])
					}
				}
			}
		})
	}
}

func TestChunkSliceWithSize_Panic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("ChunkSliceWithSize() should panic with chunk size <= 0")
		}
	}()

	ChunkSliceWithSize([]int{1, 2, 3}, 0)
}

func TestChunkSliceWithSize_RealWorldScenario(t *testing.T) {
	slice := make([]int, 5000)
	for i := range slice {
		slice[i] = i
	}

	chunkSize := 2000
	chunks := ChunkSliceWithSize(slice, chunkSize)

	if len(chunks) != 3 {
		t.Errorf("Expected 3 chunks for 5000 elements with chunk size 2000, got %d", len(chunks))
	}

	if len(chunks[0]) != 2000 {
		t.Errorf("First chunk should have 2000 elements, got %d", len(chunks[0]))
	}

	if len(chunks[1]) != 2000 {
		t.Errorf("Second chunk should have 2000 elements, got %d", len(chunks[1]))
	}

	if len(chunks[2]) != 1000 {
		t.Errorf("Third chunk should have 1000 elements, got %d", len(chunks[2]))
	}

	totalElements := 0
	for _, chunk := range chunks {
		totalElements += len(chunk)
	}

	if totalElements != 5000 {
		t.Errorf("Total elements across chunks should be 5000, got %d", totalElements)
	}
}
