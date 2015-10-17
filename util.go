package rpc

func MakeMethodName(prot string, method string) string {
	if len(prot) == 0 {
		return method
	} else {
		return prot + "." + method
	}
}

func SplitMethodName(n string) (p string, m string) {
	for i := len(n) - 1; i >= 0; i-- {
		if n[i] == '.' {
			p = n[0:i]
			if i < len(n)-1 {
				m = n[(i + 1):]
			}
			return
		}
	}
	m = n
	return
}

func runInBg(f func() error) chan error {
	done := make(chan error)
	go func() {
		done <- f()
	}()
	return done
}
