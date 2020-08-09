# Setup

    sudo apt-get install libssl-dev libgit2-dev



# TODO

Immediate:

- maybe make single file per project? This file will contain *all* the information (i.e. log, metadata, heads, etc etc. ) I think this is much much better, then from the log the project information can be constructed - we save 4x inodes at least

- log & metadata should be scanned only up to a certain time when retrieved
- add commits to the dataset
- what to do with project stats, i.e. starts, watchers, etc. 
  (idea: source in metadata, watchers.csv then in project)
- heads can be nameless, have provision for that - perhaps heads are vector really
- calculate heads in time...