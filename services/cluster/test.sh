#!/bin/bash
mix local.rebar --force
mix local.hex --force
mix deps.get
mix deps.compile
mix compile

# Run
mix test --exclude test --include v3_13_cluster