# Split Apply Combine Processing Tool

`spac` has mighty aspirations to become a general split-apply-combine
style command line processing tool for large amount of data.

For now, it's really great at selecting fields from ndjson files,
_extremely quickly_. For example, on a 109k line ndjson file,
extracting a field with `jq` takes 7s:

```
$ time cat input.json  | jq .id > /dev/null

real	0m7.599s
user	0m7.436s
sys	0m0.667s
```

With `spac`, it's an order of magnitude faster:

```
$ time cat input.json | target/release/spac select -f /id > /dev/null
1 parser error(s) -- use -v for more info

real	0m0.590s
user	0m0.472s
sys	0m0.336s
```

It achieves this performance by using the speedy
[simdjson](https://github.com/simdjson/simdjson) library.

## JSON Pointer Selectors

`spac` implements, via simdjson, the JSON pointer standard for
selecting fields. Some syntax examples can be found
[here](https://opis.io/json-schema/2.x/pointers.html).

- `/foo` extracts a field named `foo`
- `/foo/bar` extracts a field named `bar` from a map with an entry
  `foo`
- `/foo/0` treats `foo` as an array and extracts the first element

To select fields named `foo` from a collection of documents:

```
$ spac select -f /foo input.json
```

To select multiple fields:

```
$ spac select -f /foo,/bar input.json
```

## Output Options

By default, `spac` will output extracted fields, represented in JSON
sytnax, delimited by spaces. This allows you to extract sub-objects
easily. You can also output extractions as tab delimited or JSON
arrays using the `--format` option.

Because `spac` defaults to JSON syntax when outputting extractions,
strings will have quotes around them. You can drop the quotes by using
`--raw`.

## Error Handling

`spac` drops input if it can't parse a line or find the necessary
fields, which differs from `jq`'s behavior. In the future `spac` will
support optionally filling in null for missing fields.

By default, it will count up parsing errors and print them to stderr
when it's done so that you're aware if there are problems. The process
will also return an error code. To suppress this warning and return
code, use the `-q` option.

If `spac` encounters an error, it will print out the offending line if
you supply the `-v` option.

## Building

You'll need a relatively new version of Rust, its build tool Cargo, as
well as a C++ compiler capable of producing simd
instructions. Building is straightforward:

```
$ cargo build
```

Or

```
$ cargo build --release
```

To produce an optimized build. Binaries can then be found in the
`target/` subdirectories.
