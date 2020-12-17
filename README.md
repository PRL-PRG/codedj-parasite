# Setup

    sudo apt-get install libssl-dev libgit2-dev cmake pkg-config libicu-dev zlib1g-dev libcurl4-gnutls-dev ruby-dev cloc
    gem install github-linguist    

# TODO

- add snapshots analysis
- add simple csv exporter

# HOW TO

Install prerequisites:

```sh
sudo apt-get install libssl-dev libgit2-dev cmake pkg-config libicu-dev zlib1g-dev libcurl4-gnutls-dev ruby-dev cloc
sudo gem install github-linguist    
```

Due to an oversight, there's a hardcoded path to `/mnt/data/github-tokens.csv`. The file must exist, otherwise the downloader stops.

Prepare a list of projects to download in a CSV file. One column, no headers, at some location. The projects are given as URLs that can be used to clone the repo. Example contents:

```sh
https://github.com/djanco-testing/node.git
https://github.com/djanco-testing/pixi.js.git
https://github.com/djanco-testing/angular.git
https://github.com/djanco-testing/airflow.git
https://github.com/djanco-testing/react.git
https://github.com/djanco-testing/vue.git
https://github.com/djanco-testing/xonsh.git
https://github.com/djanco-testing/meteor.git
https://github.com/djanco-testing/manim.git
https://github.com/djanco-testing/Photon.git
```

Compile the downloader:

```sh
cargo build --release
```

Make directory where the dataset will live:

```sh
mkdir -p /PATH/TO/DATASET
```

Initialize the repo and add projects:

```sh
cd /PATH/TO/DATASET
path/to/dejacode-downloader/target/release/dcd init .
cd ../DATASET # apparently the downloader program messes with $PWD, this hack fixes it
path/to/dejacode-downloader/target/release/dcd add SOMEWHERE/list.csv
cd ../DATASET # it's weird, but it works
```

Start download:

```sh
path/to/dejacode-downloader/target/release/dcd update
```

Done.

