# Setup

    sudo apt-get install libssl-dev libgit2-dev cmake pkg-config libicu-dev zlib1g-dev libcurl4-gnutls-dev ruby-dev cloc
    gem install github-linguist    



# TODO


- to drop substore, all are paused, and we have to wake up one thread... 

- make sure that all dbs first write to the file and only then write the index to the indexer so that if there is an during the write, the indexer won't change to bad contents

- force update should do all commits





- load the counts and then start immediately in command mode
- utilize alt mode, i.e. output log to the alt mode, have a log in the normal mode
- threads & tasks

## Projects

## Minimizing Memory Footprint


- projects: instead of full git URL remember project type (Github, Bitbucket, etc) and a smaller string, i.e. for github just username and repo name, then construct the url when needed

- commits: hash (20 bytes) to id (8 bytes), there is no way to make this cheaper

- hashes : hash (20 bytes) to id (8 bytes) + whether contents has been stored (no id, 1 byte)

- contents : id - hash (8 bytes) to id (8 bytes), but we don't need to keep this in memory (used as 1 byte it hashes map)

- users : email (?) -> id (4 bytes)


