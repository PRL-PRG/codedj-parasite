# CodeDJ - Parasite v4

Parasite is responsible for the implementation & updates to the append only datastore of software repositories. This is the fourth version, which hopefully will be final, for some time...

## Code Overview

Code is in the `src` folder. Here is quick description of the files it contains:

- `lib.rs` - the parasite library that provides read and write access functions to the datastores. Stuff useful for parasite, mistletoe and djanco goes here. 
- `parasite.rs` - command-line utility for dataset maintenance and updater. Any commands that require write-access to the datastore go here. 
- `mistletoe.rs` - command-line read-only client for reading raw datastore contents. Any commands that only read the stored information about projects go here. 
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

## TL;DR: Major changes from version 3

- complete overhaul of internal representation (append only tables and various indexers as opposed to stores, linked stores, split stores, etc.)
- substores are independent datastores that just happen to share project ids across themselves, but each substore has its own savepoints, etc. 
- much more resilient and robust
- minimalist to the bone

## Datastore



## Parasite

## Mistletoe


## Questions

Perhaps interesting idea that would get rid of merghing: We can override how projects are assigned to datastores and relax the requirement that a project belongs to one datastore at a time only. If we keep the guarantee that project ids are deduplicated for the CodeDJ superstore, then for instance we can store V8 in both JavaScript and C++ substores. This would mean that our datastores will be larger on disk, but maybe not by that much... 

OTOH if you want projects with *any* number of files in given language, you still have to merge/process multiple... 

## TODO

