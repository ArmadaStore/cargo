FROM golang
WORKDIR /app
ADD go.mod go.sum /app/
RUN go mod download
ADD . /app/
RUN go build -o cargo cmd/cargo/cargo.go
ENTRYPOINT ["./cargo"]