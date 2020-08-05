use dcd::downloader_state::*;
use dcd::ghtorrent::*;

fn main() {
    let mut dcd = DownloaderState::create_new("/dejavuii/dejacode/dataset");
    let mut ght = GHTorrent::new("/dejavuii/dejacode/ghtorrent/dump");
    ght.add_projects(& mut dcd);
}