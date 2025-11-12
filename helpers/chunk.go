package helpers

func ChunkSliceWithSize[T any](slice []T, chunkSize int) [][]T {
	if chunkSize <= 0 {
		panic("chunk size must be greater than 0")
	}

	if len(slice) == 0 {
		return [][]T{}
	}

	chunks := make([][]T, 0, (len(slice)+chunkSize-1)/chunkSize)

	for i := 0; i < len(slice); i += chunkSize {
		end := i + chunkSize
		if end > len(slice) {
			end = len(slice)
		}
		chunks = append(chunks, slice[i:end])
	}

	return chunks
}
