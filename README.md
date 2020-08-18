# Setup

    sudo apt-get install libssl-dev libgit2-dev

# Tools 

`dcd-init` = Initializes new database in given path. 
`dcd-add` = adds new commits to specified database. The commits are supposed to live in a csv file with single column with the full url. The file is expected to have a header. 
`dcd-ghtorrent` = adds projects from ghtorrent dump (or its subset) to the database
`dcd` = the in the future incremental downloader. clones all commits in the database and updates their contents
`dcd-merge` = merges two datasets into one

The following are extra tools:

`tools/filter` = filters ghtorrent based on either language, or first M projects
`tools/random-filter` = filters random N projects with at least M commits for a given lanaguage
`tools/random-filter-toplas` = filters random N projects with at least M commits for all languages in the TOPLAS paper

# Datasets

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