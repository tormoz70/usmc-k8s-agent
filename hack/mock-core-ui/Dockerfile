FROM golang:1.22 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY internal ./internal
COPY hack/mockcorelib ./hack/mockcorelib
COPY hack/mock-core-ui ./hack/mock-core-ui
COPY deploy/base/policy ./deploy/base/policy
RUN CGO_ENABLED=0 go build -o /mock-core-ui ./hack/mock-core-ui

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /mock-core-ui /mock-core-ui
COPY --from=build /src/deploy/base/policy /policy
EXPOSE 8090
USER nonroot:nonroot
ENV FEATURES_DIR=/policy
ENTRYPOINT ["/mock-core-ui"]
