package alirmq

func StringSet(ss []string) []string {
	m := make(map[string]struct{})
	for _, s := range ss {
		m[s] = struct{}{}
	}
	res := make([]string, 0, len(m))
	for s := range m {
		res = append(res, s)
	}
	return res
}
