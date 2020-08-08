#include <filesystem>
#include <unordered_set>

#include "helpers.h"
#include "csv.h"


std::string InputDir = "/dejavuii/dejacode/ghtorrent/dump";

std::string OutputDir = "/dejavuii/dejacode/ghtorrent/dump-cpp";


struct Done {};



/** Filters first N projects and returns their ids...
 */
std::unordered_set<uint64_t> FilterFirstProjects(size_t n) {
    std::cout << "Filtering projects..." << std::endl;
    std::unordered_set<uint64_t> result;
    std::ofstream w{OutputDir + "/projects.csv"};
    w << "id,url,ownerId,name,desc,lang,createdAt,forkedFrom,deleted,updatedAt,forkedCommitId" << std::endl;
    try {
        CSVReader::Parse(InputDir + "/projects.csv", [&](std::vector<std::string> & row) {
            if (--n == 0)
                throw Done();
            result.insert(std::stoull(row[0]));
            w << row[0] << "," // id
              << EscapeQuotes(row[1]) << "," // url
              << row[2] << "," // owner_id
              << EscapeQuotes(row[3]) << "," // name
              << "\"\"," // desc (ignored)
              << EscapeQuotes(row[5]) << "," // lang
              << EscapeQuotes(row[6]) << "," // created at
              << row[7] << "," // forked from
              << row[8] << "," // deleted
              << EscapeQuotes(row[9]) << "," // updated_at
              << "\"\"" << std::endl; // forked_commit_id (ignored)
        });
    } catch (Done const &) {
    }
    return result;
}

/** Filters only projects of certain language.

    To keep the contents as small as possible for now we also filter out any forks and deleted projects.
 */
std::unordered_set<uint64_t> FilterLanguageProjects(std::string const & language) {
    std::cout << "Filtering projects..." << std::endl;
    std::unordered_set<uint64_t> result;
    std::ofstream w{OutputDir + "/projects.csv"};
    w << "id,url,ownerId,name,desc,lang,createdAt,forkedFrom,deleted,updatedAt,forkedCommitId" << std::endl;
    CSVReader::Parse(InputDir + "/projects.csv", [&](std::vector<std::string> & row) {
        if (row[5] != language || row[8] == "1" || row[7] != "\\N")
            return;
        result.insert(std::stoull(row[0]));
        w << row[0] << "," // id
          << EscapeQuotes(row[1]) << "," // url
          << row[2] << "," // owner_id
          << EscapeQuotes(row[3]) << "," // name
          << "\"\"," // desc (ignored)
          << EscapeQuotes(row[5]) << "," // lang
          << EscapeQuotes(row[6]) << "," // created at
          << row[7] << "," // forked from
          << row[8] << "," // deleted
          << EscapeQuotes(row[9]) << "," // updated_at
          << "\"\"" << std::endl; // forked_commit_id (ignored)
    });
    return result;
}

void FilterDataset(std::unordered_set<uint64_t> & valid_projects) {
    // first determine valid commits and update project dataset
    std::unordered_set<uint64_t> valid_commits;
    std::unordered_set<uint64_t> valid_users;
    {
        std::cout << "Filtering project commits..." << std::endl;
        std::ofstream w{OutputDir + "/project_commits.csv"};
        CSVReader::Parse(InputDir + "/project_commits.csv", [&](std::vector<std::string> & row) {
            if (valid_projects.find(std::stoull(row[0])) != valid_projects.end()) {
                w << row[0] << "," // project id
                  << row[1] << std::endl; // commit id
                valid_commits.insert(std::stoull(row[1]));
            }
       }, /* headers */ false);
    }
    valid_projects.clear(); // no longer needed
    {
        std::cout << "Filtering commit details..." << std::endl;
        std::ofstream w{OutputDir + "/commits.csv"};
        CSVReader::Parse(InputDir + "/commits.csv", [&](std::vector<std::string> & row) {
            if (valid_commits.find(std::stoull(row[0])) != valid_commits.end()) {
                w << row[0] << "," // commitId
                  << row[1] << "," // hash
                  << row[2] << "," // authorId
                  << row[3] << "," // committerId
                  << row[4] << "," // projectId
                  << EscapeQuotes(row[5]) << std::endl; // createdAt
                valid_users.insert(std::stoull(row[2]));
                valid_users.insert(std::stoull(row[3]));
            }
       }, /* headers */ false);
    }
    {
        std::cout << "Filtering commit parents..." << std::endl;
        std::ofstream w{OutputDir + "/commit_parents.csv"};
        CSVReader::Parse(InputDir + "/commit_parents.csv", [&](std::vector<std::string> & row) {
            if (valid_commits.find(std::stoull(row[0])) != valid_commits.end()) {
                // note that there are issues in ghtorrent database and in this step we can reference commits that have not been selected before, so the subsequent ghtorrent analyzer must deal with this and be ready to process incomplete data. To keep as close to real ghtorrent as possible, the filter is not cleaning the data
                w << row[0] << "," // commitId
                  << row[1] << std::endl; // parentId
            }
       }, /* headers */ false);
    }
    valid_commits.clear(); // no longer needed
    {
        std::cout << "Filtering users..." << std::endl;
        std::ofstream w{OutputDir + "/users.csv"};
        w << "id,login,company,createdAt,type,fake,deleted,long,lat,countryCode,state,city,location" << std::endl;
        CSVReader::Parse(InputDir + "/users.csv", [&](std::vector<std::string> & row) {
            if (valid_users.find(std::stoull(row[0])) != valid_users.end()) {
                w << row[0] << "," // id
                  << EscapeQuotes(row[1]) << "," // login
                  << "\"\"," // company (ignored)
                  << EscapeQuotes(row[3]) << "," // createdAt
                  << "\"\"," // type (ignored)
                  << "\"\"," // fake (ignored)
                  << "\"\"," // deleted (ignored)
                  << "\"\"," // long (ignored)
                  << "\"\"," // lat (ignored)
                  << "\"\"," // countryCode (ignored)
                  << "\"\"," // state (ignored)
                  << "\"\"," // city (ignored)
                  << "\"\"" << std::endl; // location (ignored)
            }
       });
    }
}

int main() {
    std::filesystem::create_directories(OutputDir);
    //std::unordered_set<uint64_t> validProjects{FilterFirstProjects(10000)};
    std::unordered_set<uint64_t> validProjects{FilterLanguageProjects("C++")};
    FilterDataset(validProjects);
    return EXIT_SUCCESS;
}
