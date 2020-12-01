# Setup

    sudo apt-get install libssl-dev libgit2-dev cmake pkg-config libicu-dev zlib1g-dev libcurl4-gnutls-dev ruby-dev cloc
    gem install github-linguist    

# Basics

> TODO basic info about code and data layout.

# Features to be added

- github linguist and cloc to be executed on snapshots
- db guys should be able to regenerate their indices from the datafiles
- a datastore should be able to rollback to any given savepoint
- add helper functions to show savepoints, enumerate items, etc. 
- add projects from github directly
- add issues


# TODO

- and how to stop them threads that are updating (purge the queue should do it, the task would then end once all other threads will become idle)

- add log for commands entered
- add log for on/off of the downloader on the database


- force update should do all commits


## Projects

## Minimizing Memory Footprint


- projects: instead of full git URL remember project type (Github, Bitbucket, etc) and a smaller string, i.e. for github just username and repo name, then construct the url when needed

- commits: hash (20 bytes) to id (8 bytes), there is no way to make this cheaper

- hashes : hash (20 bytes) to id (8 bytes) + whether contents has been stored (no id, 1 byte)

- contents : id - hash (8 bytes) to id (8 bytes), but we don't need to keep this in memory (used as 1 byte it hashes map)

- users : email (?) -> id (4 bytes)


