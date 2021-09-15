use git_version::git_version;

pub const VERSION: &'static str = env!("CARGO_PKG_VERSION");
pub const GIT_VERSION: &str = git_version!(args = ["--dirty=*", "--abbrev=40", "--always"]);

pub fn stamp() -> String {
    return format!("{} @{}", VERSION, GIT_VERSION);
}

pub fn is_modified_version() -> bool {
    return GIT_VERSION.ends_with("*");
}

