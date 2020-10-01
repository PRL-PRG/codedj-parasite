# Setup

    sudo apt-get install libssl-dev libgit2-dev cmake pkg-config libicu-dev zlib1g-dev libcurl4-gnutls-dev ruby-dev cloc
    gem install github-linguist    

# TODO

- figure out the single mutex & conditional variable stuff for process queue and task managers...
- I guess the process queue will have to be integrated with the tasks manager so that its own mutex can be used :( ugly ugly design

- add snapshots analysis
- add simple csv exporter

