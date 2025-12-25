package guard

func Assert(b bool, message string) {
	if b {
		return
	}

	panic(message)
}
