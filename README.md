# CodeDJ - Parasite v4

Parasite is responsible for the implementation & updates to the append only datastore of software repositories. This is the fourth version, which hopefully will be final, for some time...

# Setup 

Install prerequisites.

> Note this list is not exhaustive as I haven't tried to build on a clean machine, if you do, please add any required packages here too. 

    sudo apt install git libssl-dev
    
Then install rust:

    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

After which it is recommended to reboot (a logoff/login should be enough in theory). Then build:

    cargo build --release

(or omit release for debug build). The resulting binaries will be found in `target/release` or `target/debug`. 


## Parasite

Usage:

    parasite ARGS cmd CMD_ARGS

where `ARGS` are:

- `--datastore`, or `-ds` to specify the folder in which the CodeDJ datastore to be operated on exists/will be created
- `--verbose`, or `-v`, or `-vv` to specify the verbosity level
- `--force` to force parasite to perform the required command even against its better judgement. Use with extreme caution, most likely you will destroy the store irrepairably by using this option

These arguments are followed by a command, which is followed by command specific options. The following commands are supported:

#### `create`

Creates a CodeDJ superstore in the specified folder. This can fail for numerous reasons, such as dirty build, already existing folder, etc. Can be forced.

#### `log`

Displays a full command log of an existing CodeDJ superstore.

## Mistletoe


## Code Overview

Code is in the `src` folder. Here is quick description of the files it contains:

- `lib.rs` - the parasite library that provides read and write access functions to the datastores. Stuff useful for parasite, mistletoe and djanco goes here. 
- `datastore.rs` - datastore implementation and basic maintenance
- `datastore_view.rs` - the readonly datastore view implementation
- `records.rs` - structs that the datastore keeps
- `savepoints.rs` - savepoints
- `folder_lock.rs` - simple RAII folder lock that makes sure only one instance has write access to a folder
- `serialization.rs` - serialization trait used by the datastore to save and read data
- `stamp.rs` - stamp (version, git commit, etc.) so that we can make sure that only code for which we have a record does modifications on datastores
- `table_writer.rs` - basic infrastructure for the append only table and the append only table itself. 
- `table_readers.rs` - various indexed readers of an append only table
- `codedj.rs` - the super store that is a set of datastores for the various languages we keep as well as other bookkeeping and metadata required for reliability and maintenance (command logs, etc.) and extra information from other sources, such as the downloader's metadata and indices, discovered projects, etc. Note that CodeDJ is only useful in write access mode, for reading purposes only, the datastoreViews should be used. 

The `parasite` subfolder contains files used by parasite only, these are:

- `parasite-cli.yaml` - YAML description of parasite's command line interface
- `parasite.rs` - command-line utility for dataset maintenance and updater. Any commands that require write-access to the datastore go here. 
- `updater.rs` - the updater task responsible for updating a repository in a datastore

And finally the `mistletoe` folder contains files used by mistletoe only:

- `mistletoe.rs` - command-line read-only client for reading raw datastore contents. Any commands that only read the stored information about projects go here. 

## CodeDJ super store

## Datastores

## Repository Update





## Questions

Perhaps interesting idea that would get rid of merghing: We can override how projects are assigned to datastores and relax the requirement that a project belongs to one datastore at a time only. If we keep the guarantee that project ids are deduplicated for the CodeDJ superstore, then for instance we can store V8 in both JavaScript and C++ substores. This would mean that our datastores will be larger on disk, but maybe not by that much... OTOH if you want projects with *any* number of files in given language, you still have to merge/process multiple... 

Shoudl CodeDJ also manage all datastore views to provided savepoints, or should we have this on the datastore users themselves? I am slightly leaning towards the themselves option, but only very slightly...

## TODO

