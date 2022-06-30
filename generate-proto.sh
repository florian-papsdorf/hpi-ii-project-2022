#!/bin/bash

protoc --proto_path=proto --python_out=build/gen proto/bakdata/corporate/v1/corporate.proto
protoc --proto_path=proto --python_out=build/gen proto/bakdata/lobbyist/v1/lobbyist.proto
protoc --proto_path=proto --python_out=build/gen proto/bakdata/lobbyist/v2/lobbyist.proto
protoc --proto_path=proto --python_out=build/gen proto/bakdata/corporate_detailed/v1/corporate_detailed.proto
protoc --proto_path=proto --python_out=build/gen proto/bakdata/fold_out/v1/fold_out.proto

