# Benchmark Instructions

## Tool

- Use Go's built-in testing benchmarks
- Bash scripts

## Running

go test -bench=. -benchmem -count=5 -benchtime=3s ./benchmarks/

## Required flags

- `-count=5` always — variance across runs matters
- `-benchmem` always — allocations are a key metric
- `-benchtime=3s` minimum — short runs give noisy results

## What to measure

Every benchmark must report at minimum:

- ns/op (latency)
- MB/s (throughput, use b.SetBytes())
- allocs/op (allocation pressure)
- latency, packet loss, bandwidth limit, jitter/reorder and other metrics.

## Result format

After every run, save output to:
benchmarks/results/

## Plotting

After saving results, generate a plot using tools like gonum/plot or others that would give nice render in the thesis paper.
add plots and tables and other diagrams explaning things in the latex thesis.

## Thesis update trigger

After every benchmark run, update thesis/chapters/05_evaluation.tex with
the new numbers. Never leave benchmark results unrecorded in the thesis.

# NOTES

- it would be nice if different kinds of examples are added in ../example/ covering different scenarios (as individual golang files) or some unit examples (go files) that can be run mutlipletimes concurrently or sth with script files adn options
- I want the scipt @benchmarks/scenarios/run.sh to be able to take in
  different options to run and test the whole library in different modes
  and for different metrices. currently I want it to have two modes, dev
  and prod. in dev mode, it will test them using the current docker setup. in prod mode, I was thinking if it would check and install mininet( I
  think it would be a good option to use it to illustrate real world
  senarios and more resillient than docker containers, if you have other
  option other than the two, be my guest) and it will multiple nodes. multiple pubs and subs on single machine and communicate over the connection and record their data, take average or sth and report on thesis paper: for prod option for example, machine A can run two 10 pubs and 30 subs and Machine B runs the same amount. subs subscribe to mutliple pubs in both machines and everything is loged and later structured and reported in the thesis. this can be done with different metrics like latency, packet loss....

- all of this should be documented on how to use the script, either as a help option or separate .md file for it.
- when running, quic uses tls while tcp uses curve and it won't be fair to compare the two head to head so, if there is a way to run and measure only the part after handshake and all the encryption headstart which could vary between the two. I think this could be possible if we take the time stamps from the logs/qlogs the library provides. this isn't replacing the existing test of total time but in addition to it.
- record latency, packet loss, bandwidth limit, jitter/reorder and other metrics.
