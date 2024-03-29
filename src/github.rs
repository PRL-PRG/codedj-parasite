use std::sync::*;

use crate::helpers;
use crate::settings::SETTINGS;
use crate::updater::*;
use crate::LOG;

/** Access to github api. 
 
    - rotate tokens
 */

 use curl::easy::*;


pub struct Github {
    tokens : Mutex<TokensManager>,
}

impl Github {

    pub fn new(tokens : & str) -> Github {
        return Github{
            tokens : Mutex::new(TokensManager::new(tokens)),
        }
    }

    /** Gets the repository information for given repository. 
     */
    pub fn get_repo(& self, user_and_repo : & str, task : Option<& TaskStatus>) -> Result<json::JsonValue, std::io::Error> {
        return self.request(& format!("https://api.github.com/repos/{}", user_and_repo), task);
    }

    /** Performs a github request of the specified url and returns the result string.  
     */
    pub fn request(& self, url : & str, task : Option<& TaskStatus>) -> Result<json::JsonValue, std::io::Error> {
        let mut attempts = 0;
        let max_attempts = self.tokens.lock().unwrap().len();
        loop {
            let mut response = Vec::new();
            let mut response_headers = Vec::new();
            let mut conn = Easy::new();
            conn.url(url)?;
            conn.follow_location(true)?;
            let mut headers = List::new();
            headers.append("User-Agent: dcd").unwrap();
            let token = self.tokens.lock().unwrap().get_token();
            headers.append(& format!("Authorization: token {}", token.0)).unwrap();
            conn.http_headers(headers)?;
            {
                let mut ct = conn.transfer();
                ct.write_function(|data| {
                    response.extend_from_slice(data);
                    return Ok(data.len());
                })?;
                ct.header_function(|data| {
                    response_headers.extend_from_slice(data);
                    return true;
                })?;
                ct.perform()?;
            }
            let rhdr = helpers::to_string(& response_headers).to_lowercase();
            if rhdr.starts_with("http/1.1 200") || rhdr.starts_with("http/1.1 301") || rhdr.starts_with("http/2 200") || rhdr.starts_with("http/2 301") {
                let result = json::parse(& helpers::to_string(& response));
                match result {
                    Ok(value) => return Ok(value),
                    Err(_) => {
                        return Err(std::io::Error::new(std::io::ErrorKind::Other, "Cannot parse json result"));
                    }
                }
            } else if rhdr.starts_with("http/1.1 401") || rhdr.starts_with("http/1.1 403") || rhdr.starts_with("http/2 401") || rhdr.starts_with("http/2 403") {
                if rhdr.contains("x-ratelimit-remaining: 0") {
                    // move to next token
                    self.tokens.lock().unwrap().next_token(token.1);
                    task.map(|t| { t.info("moving to next Github API token") });
                // check for the secondary rate limit:)
                } else {
                    let result = json::parse(& helpers::to_string(& response));
                    match result {
                        Ok(value) => {
                            if value["message"].is_string() && value["message"].as_str().unwrap() == "You have exceeded a secondary rate limit. Please wait a few minutes before you try again." {
                                println!("Secondary rate limit: sleep 1m");
                                std::thread::sleep(std::time::Duration::from_millis(1000 * 60));
                            }
                        }
                        Err(_) => {
                        }
                    }
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, rhdr.split("\n").next().unwrap()));
                }
            } else{
                return Err(std::io::Error::new(std::io::ErrorKind::Other, rhdr.split("\n").next().unwrap()));
            }
            attempts += 1;
            // if we have too many attempts, it likely means that the tokens are all used up, wait 10 minutes is primitive and should work alright...
            if attempts == max_attempts {
                task.map(|t| { t.info(format!("all Github API tokens ({}) exhausted, sleeping for 10 minutes", max_attempts)) });
                std::thread::sleep(std::time::Duration::from_millis(1000 * 60 * 10));
                attempts = 0;
            }
        }
    }
}

struct TokensManager {
    tokens : Vec<String>,
    current : usize,
}

impl TokensManager {
    fn new(filename : & str) -> TokensManager {
        LOG!("Loading github access tokens from {}", filename);
        let mut reader = csv::ReaderBuilder::new()
            .has_headers(true)
            .double_quote(false)
            .escape(Some(b'\\'))
            .from_path(filename).unwrap();
        let mut tokens = Vec::<String>::new();
        for x in reader.records() {
            tokens.push(String::from(& x.unwrap()[0]));
        }
        LOG!("    {} tokens found", tokens.len());
        return TokensManager{
            tokens, 
            current : 0,
        };
    }

    fn len(& self) -> usize {
        return self.tokens.len();
    }

    /** Returns a possibly valid token that should be used for the request and its id. 
     */ 
    fn get_token(& mut self) -> (String, usize) {
        return (self.tokens[self.current].clone(), self.current);
    }

    fn next_token(& mut self, id : usize) {
        if self.current == id {
            self.current += 1;
            if self.current == self.tokens.len() {
                self.current = 0;
            }
        }
    }
}
