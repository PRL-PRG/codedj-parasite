use dcd::downloader_state::*;
use dcd::ghtorrent;

fn main() {
    let mut dcd = DownloaderState::create_new("/dejavuii/dejacode/dataset");
    ghtorrent::import("/dejavuii/dejacode/ghtorrent/dump", & mut dcd);
}