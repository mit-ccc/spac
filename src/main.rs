use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};

use clap::{AppSettings, Clap};
use simdjson_rust::dom::element::{Element, ElementType};

fn extract_fields(
    doc: &Element,
    pointers: &Vec<&str>,
    drop_quotes: bool,
) -> Result<Vec<String>, &'static str> {
    let mut results = Vec::new();
    for p in pointers {
        let el = doc.at_pointer(p);
        match el {
            Ok(v) => {
                let v = match drop_quotes {
                    true => unquote_str(v.get_type(), &v.minify()).to_string(),
                    false => v.minify().to_string(),
                };
                results.push(v);
            }
            Err(_error) => {
                return Err("parse error");
            }
        };
    }
    return Ok(results);
}

// drop first and last quotes if string, otherwise just pass through
fn unquote_str<'a>(el_type: ElementType, s: &'a String) -> &'a str {
    match el_type {
        ElementType::String => &s[1..(s.len() - 1)],
        _ => &s,
    }
}

enum FormatType {
    Json,
    Tab,
    Space,
}

impl FormatType {
    fn from(s: &String) -> Result<FormatType, &'static str> {
        match &s[..] {
            "json" => Ok(FormatType::Json),
            "tab" => Ok(FormatType::Tab),
            "space" => Ok(FormatType::Space),
            _ => Err("unknown format type"),
        }
    }
}

fn extract(
    input: impl Iterator<Item = String>,
    pointers: &Vec<&str>,
    drop_quotes: bool,
    suppress_errors: bool,
    verbosity: u32,
    format_type: FormatType,
) -> bool {
    if pointers.len() == 0 {
        panic!("extract needs pointers");
    }

    let verbosity = if suppress_errors { 0 } else { verbosity };
    let drop_quotes = match format_type {
        FormatType::Json => false,
        _ => drop_quotes,
    };

    let mut error_count = 0;
    let mut parser = simdjson_rust::dom::Parser::default();
    for line in input {
        if line.is_empty() {
            continue;
        }
        let doc = parser.parse(&line);
        let doc = match doc {
            Ok(val) => val,
            Err(_e) => {
                error_count += 1;
                if verbosity > 0 {
                    eprintln!("parse error on line: {}", line);
                }
                continue;
            }
        };

        let fields = extract_fields(&doc, pointers, drop_quotes);
        let fields = match fields {
            Ok(f) => f,
            Err(_e) => {
                error_count += 1;
                if verbosity > 0 {
                    eprintln!("missing field on line: {}", line);
                }
                continue;
            }
        };

        match format_type {
            FormatType::Tab => println!("{}", fields.join("\t")),
            FormatType::Json => println!("[{}]", fields.join(",")),
            _ => println!("{}", fields.join(" ")),
        };
    }

    if error_count > 0 {
        if suppress_errors {
            return true;
        } else {
            eprintln!("{} parser error(s) -- use -v for more info", error_count);
            return false;
        }
    }

    return true;
}

fn stream_stdin() -> impl Iterator<Item = String> {
    let file = io::stdin();
    let reader = BufReader::new(file);

    let iter = reader
        .lines()
        .filter(|line| line.is_ok())
        .map(|line| line.unwrap());
    return iter;
}

fn stream_file(fname: String) -> impl Iterator<Item = String> {
    let file = match File::open(&fname) {
        Ok(f) => f,
        Err(_err) => {
            println!("unable to open file {}", fname);
            panic!("missing input file");
        }
    };
    let reader = BufReader::new(file);

    let iter = reader
        .lines()
        .filter(|line| line.is_ok())
        .map(|line| line.unwrap());
    return iter;
}

fn stream_files(fnames: Vec<String>) -> impl Iterator<Item = String> {
    let iters = fnames.into_iter().map(|fname| stream_file(fname));
    iters.flat_map(|it| it)
}

#[derive(Clap)]
#[clap(version = "(build)", author = "Wes Chow <wesc@media.mit.edu>")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(subcommand)]
    select: SelectCommand,
}

#[derive(Clap)]
enum SelectCommand {
    /// field selector
    Select(SelectOpts),
}

#[derive(Clap)]
struct SelectOpts {
    /// input files, leave blank for stdin
    input: Vec<String>,
    /// list of JSON pointer formatted selectors, comma separated
    #[clap(short, long, required(true))]
    fields: String,
    /// raw string output (ignored if format output is json)
    #[clap(short, long)]
    raw: bool,
    /// suppress warnings
    #[clap(short, long)]
    quiet: bool,
    /// verbosity level
    #[clap(short, long, parse(from_occurrences))]
    verbose: u32,
    /// use tab separated format output
    #[clap(long, possible_values(&["space", "tab", "json"]), default_value("space"))]
    format: String,
}

fn run_app() -> bool {
    let opts: Opts = Opts::parse();
    match opts.select {
        SelectCommand::Select(opts) => {
            let pointers: Vec<&str> = opts.fields.split(",").collect();
            if opts.raw && opts.format == "json" {
                eprintln!("warning: --raw has no effect when using json formatting")
            }
            if opts.input.len() == 0 {
                return extract(
                    stream_stdin(),
                    &pointers,
                    opts.raw,
                    opts.quiet,
                    opts.verbose,
                    FormatType::from(&opts.format).unwrap(),
                );
            } else {
                return extract(
                    stream_files(opts.input),
                    &pointers,
                    opts.raw,
                    opts.quiet,
                    opts.verbose,
                    FormatType::from(&opts.format).unwrap(),
                );
            }
        }
    }
}

fn main() {
    std::process::exit(if run_app() { 0 } else { 1 });
}
