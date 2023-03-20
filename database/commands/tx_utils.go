package commands

func readFirstKey(args [][]byte) ([]string, []string) {
	// assert len(args) > 0
	key := string(args[0])
	return nil, []string{key}
}

func writeFirstKey(args [][]byte) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

func prepareSetCalculate(args [][]byte) ([]string, []string) {
	keys := make([]string, len(args))
	for i, arg := range args {
		keys[i] = string(arg)
	}
	return nil, keys
}

func prepareSetCalculateStore(args [][]byte) ([]string, []string) {
	dest := string(args[0])
	keys := make([]string, len(args)-1)
	keyArgs := args[1:]
	for i, arg := range keyArgs {
		keys[i] = string(arg)
	}
	return []string{dest}, keys
}
