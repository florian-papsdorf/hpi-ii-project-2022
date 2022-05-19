#!/bin/bash

protoc --proto_path=proto --python_out=build/gen proto/bakdata/corporate/v1/corporate.proto
protoc --proto_path=proto --python_out=build/gen proto/bakdata/lobbyist/v1/lobbyist.proto
protoc --proto_path=proto --python_out=build/gen proto/bakdata/lobbyist/v2/lobbyist.proto
