CARGO := cargo

.PHONY: all setup build run cli test clean

all: build

# Fetch the bundled python-build-standalone interpreter into vendor/python.
# Required once before the first build (PyO3 links against it).
setup:
	scripts/fetch-python.sh

build:
	$(CARGO) build --release

# Run the GUI.
run:
	$(CARGO) run --bin gui

# Run the CLI.
cli:
	$(CARGO) run --bin cli

test:
	$(CARGO) test

clean:
	$(CARGO) clean
