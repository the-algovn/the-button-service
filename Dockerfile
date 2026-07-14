FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/the-button-service ./cmd/the-button-service

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/the-button-service /the-button-service
ENTRYPOINT ["/the-button-service"]
