package device

import "sort"

func sortDevices(ds []*Device) {
	sort.Slice(ds, func(i, j int) bool { return ds[i].ID < ds[j].ID })
}
