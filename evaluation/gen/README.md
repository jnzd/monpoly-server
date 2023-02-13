# Genereta Random Traces and Formulas

## To generate random MFOTL traces, the usage is as follows:

```
./gen sig mfotl trace nmax n p l of
```

where
- `sig` is the signature file
- `nmax` is an integer such that all event arguments are drawn uniformly from `{0..nmax-1}`
- `n` and `p` are respectively an integer and a float such that every timepoint contains Binom(`n`,`p`) events (event names are drawn uniformly)
- `l` is the desired length of the trace
- `of` is the output file

The generated trace will contain a trace with timepoints `{0..l-1}` with timepoint `i` having timestamp `i`, and be written out to `of.monpoly.trc`.

To generate random LTL traces, the usage is

```
./gen sig mfotl trace n p l of
```
or

```
./gen sig mfotl trace_json n p l of
```

Two random traces are generated: one in MonPoly format (in file `of.monpoly.trc`) and one in JSON format  (in file `of.monpoly.json`).

You can also use the script to generate random monitorable MFOTL formulas.  The usage is

```
./gen sig mfotl policy d maxbound
```

where
- `sig` is the signature file
- `d` is the maximal depth of the formula (constructors are drawn uniformly)
- `maxbound` is the maximal size of intervals.