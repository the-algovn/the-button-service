FROM golang:1.26-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/the-button-service ./cmd/the-button-service \
 && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o /out/the-button-publisher ./cmd/the-button-publisher

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/the-button-service /the-button-service
COPY --from=build /out/the-button-publisher /the-button-publisher
ENTRYPOINT ["/the-button-service"]
