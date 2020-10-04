package utils

import "strconv"

func StringsToInts(strs []string) ([]int, error) {
	nums := make([]int, len(strs), len(strs))
	for i, s := range strs {
		n, err := strconv.Atoi(s)
		if err != nil {
			return nums, err
		}
		nums[i] = n
	}
	return nums, nil
}

func StringsToFloats(strs []string) ([]float64, error) {
	nums := make([]float64, len(strs), len(strs))
	for i, s := range strs {
		n, err := strconv.ParseFloat(s, 64)
		if err != nil {
			return nums, err
		}
		nums[i] = n
	}
	return nums, nil
}
