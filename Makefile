.PHONY: test demo bench all

test:
	julia --project=. -e 'using Pkg; Pkg.test()'

demo:
	julia --project=. scripts/demo.jl

bench:
	julia --project=. benchmark/run_benchmarks.jl

all: test bench demo
