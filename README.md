# Setup

    sudo apt-get install libssl-dev libgit2-dev

# Tools 

`dcd-init` = Initializes new database in given path. 
`dcd-add` = adds new commits to specified database. The commits are supposed to live in a csv file with single column with the full url. The file is expected to have a header. 
`dcd-ghtorrent` = adds projects from ghtorrent dump (or its subset) to the database
`dcd` = the in the future incremental downloader. clones all commits in the database and updates their contents
`dcd-merge` = merges two datasets into one
`dcd-verify` = verifies the integrity of a specified dataset
`dcd-describe` = describes given dataset
`dcd-issues` = augments given dataset with issues information from ghtorrent
`dcd-export` = exports the CSV for artifact (project, commit, language counts)

# Not used anymore

`dcd-repair` = fixed error in projects logs for the earliest datasets

The following are extra tools:

`tools/filter` = filters ghtorrent based on either language, or first M projects
`tools/random-filter` = filters random N projects with at least M commits for a given lanaguage
`tools/random-filter-toplas` = filters random N projects with at least M commits for all languages in the TOPLAS paper

# Datasets

For time constraints, we have parallelized and distributed a lot of the dataset creation, merging the result in the end. This describes the partial datasets and how they are merged together. Note that once the datasets are merged, this information is invalid. 

> Due to a bug in Rust's CSV parses escaping of quotes and escapes, which we only discovered much later in the pipeline, the precise path and user names cannot be obtained if they contain the special characters. However, this has no effect on the data calculated by the paper because their extensions and identities are untouched. 

> Due to the local nature of this, all paths are absolute wrt where the datasets can be found on our dejavuii server. 

`/dejacode/partial_datasets/dcd-1000` = a sample dataset of 1k projects > 28 commits for the 17 languages
`/dejacode/partial_datasets/dcd-10000-c-java` = 10k projects per lang > 28 commits, C, C++, C#, ObjC, Go, Java
`/dejacode/partial_datasets/dcd-10000-coffee-ts` = 10k projects per lang > 28 commits, CoffeeScript, JavaScript, Typescript
`/dejacode/partial_datasets/dcd-10000-ruby-py` = 10k projects per lang > 28 commits, Ruby, PHP, Python
`/dejacode/partial_datasets/dcd-10000-clojure-scala` = 10k projects per lang, > 28 commits, Clojure, Erlang, Haskell, Scala
`/dejacode/partial_datasets/dcd-10000-perl` = 10k projects > 28 commits, Perl

Together these datasets are merged into the dataset: `/dejacode/dataset-large-projects-only`

`/dejacode/partial_datasets/dcd-1000-2` to `/dejacode/partial_datasets/dcd-1000-7` contains 1k projects of all sizes for all languages but Erlang and Clojure
`/dejacode/partial_datasets/dcd-small-erlang-clojure` contain 6k projects of all sizes for Erlang and Clojure

> This is because there was a mistake when we started filtering the small languages and random sampling was disabled for Erland and Clojure because they did not have enough large projects. 

Together all these datasets are merged into the final frankendataset in `/dejacode/dataset`. 









`dcd-sample-2` = 1000 per language > 28, fixed paths, no repair needed, to be merged to the large dataset
`dcd-sample` = 10k per language, > 28, fixed paths


# TODO fixes

Done in dcd-repair, must be run on currently dowloaded dcd-sample: 

- error is reported as update with key value, should become error
- when such error is found, all log since updatestart should be deleted...

- escape usernames


# Future Features

These are the features that we are currently bound to implement in the order of their implementation:



# Distant Future Features

These will likely happen *after* the paper deadline, but should be available for the artifact deadline if the paper gets accepted:

- user friendliness (i.e. decent commandline, encapsulation of stuff, better reporting, etc.)
- file snapshots & github linguist & cloc and other tools information (not sure this is needed for the paper)
- incremental downloading

- how to do cummulative metadata? I guess special kind...


# v2

- use only binary format
- spread into multiple files? maybe conditionally
- have 2 sets of API - full read and random access