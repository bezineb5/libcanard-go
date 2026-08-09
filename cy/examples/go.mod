module github.com/opencyphal/cy-go/examples

go 1.23.0

require (
	github.com/opencyphal/cy-go v0.0.0
	github.com/opencyphal/cy-go/can v0.0.0
)

require (
	go.einride.tech/can v0.17.0 // indirect
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
)

replace (
	github.com/opencyphal/cy-go => ..
	github.com/opencyphal/cy-go/can => ../can
)
