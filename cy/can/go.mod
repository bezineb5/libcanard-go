module github.com/opencyphal/cy-go/can

go 1.26.5

require (
	github.com/bezineb5/libcanard-go v0.0.0
	github.com/opencyphal/cy-go v0.0.0
	go.dw1.io/rapidhash v0.3.0
	go.einride.tech/can v0.17.0
)

require (
	golang.org/x/net v0.38.0 // indirect
	golang.org/x/sync v0.11.0 // indirect
	golang.org/x/sys v0.31.0 // indirect
)

replace github.com/bezineb5/libcanard-go => ../..

replace github.com/opencyphal/cy-go => ..
