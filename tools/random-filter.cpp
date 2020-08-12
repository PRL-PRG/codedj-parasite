#include <algorithm>
#include <iterator>
#include <random>
#include <filesystem>
#include <unordered_set>
#include <unordered_map>

#include "helpers.h"
#include "csv.h"

std::string InputDir;

std::string OutputDir;

std::unordered_map<uint64_t, std::unordered_set<uint64_t>> GetProjectIds(std::string const & language) {
    std::cout << "Filtering projects for language " << language << std::endl;
    std::unordered_map<uint64_t, std::unordered_set<uint64_t>> result;
    CSVReader::Parse(InputDir + "/projects.csv", [&](std::vector<std::string> & row) {
        if (row[5] != language || row[8] == "1" || row[7] != "\\N")
            return;
        result[std::stoull(row[0])];
    });
    std::cout << "    " << result.size() << " projects found" << std::endl;
    return result;
}

void AssignCommitsToProjects(std::unordered_map<uint64_t, std::unordered_set<uint64_t>> & projects) {
    std::cout << "Loading commits to projects..." << std::endl;
    size_t commitRecords = 0;
    CSVReader::Parse(InputDir + "/project_commits.csv", [&](std::vector<std::string> & row) {
        auto i = projects.find(std::stoull(row[0]));
        if (i != projects.end()) {
            i->second.insert(std::stoull(row[1]));
            ++commitRecords;
        }
    }, /* headers */ false);
    std::cout << "    " << commitRecords << " commit records in valid projects" << std::endl;
}

void FilterSmallProjects(std::unordered_map<uint64_t, std::unordered_set<uint64_t>> & projects, size_t cutoff) {
    std::cout << "Filtering projects with less than " << cutoff << " commits..." << std::endl;
    for (auto i = projects.begin(); i != projects.end(); ) {
        if (i->second.size() >= cutoff)
            ++i;
        else
            i = projects.erase(i);
    }
    std::cout << "    " << projects.size() << " remaining projects" << std::endl;
}

void SampleProjects(std::unordered_map<uint64_t, std::unordered_set<uint64_t>> & projects, size_t num) {
    std::cout << "Sampling projects from " << projects.size() << " to " << num << std::endl;
    if (projects.size() > num) {
        std::unordered_map<uint64_t, std::unordered_set<uint64_t>> sampled_projects;
        std::sample(projects.begin(), projects.end(), std::inserter(sampled_projects, sampled_projects.begin()) , num, std::mt19937{std::random_device{}()});
        projects = std::move(sampled_projects);
    }
    std::cout << "    " << projects.size() << " projects sampled" << std::endl;
}

void FilterProjects(std::unordered_map<uint64_t, std::unordered_set<uint64_t>> & projects) {
    std::cout << "Filtering projects..." << std::endl;
    std::ofstream w{OutputDir + "/projects.csv"};
    w << "id,url,ownerId,name,desc,lang,createdAt,forkedFrom,deleted,updatedAt,forkedCommitId" << std::endl;
    CSVReader::Parse(InputDir + "/projects.csv", [&](std::vector<std::string> & row) {
        if (projects.find(std::stoull(row[0])) != projects.end()) {
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
        }
    });
}

void FilterDataset(std::unordered_map<uint64_t, std::unordered_set<uint64_t>> & valid_projects) {
    // first determine valid commits and update project dataset
    std::unordered_set<uint64_t> valid_commits;
    std::unordered_set<uint64_t> valid_users;
    {
        std::cout << "Filtering project commits..." << std::endl;
        std::ofstream w{OutputDir + "/project_commits.csv"};
        for (auto i : valid_projects) {
            for (auto j : i.second) {
                w << i.first << "," << j << std::endl;
                valid_commits.insert(j);
            }
        }
        std::cout << "    " << valid_commits.size() << " valid commits" << std::endl;
    }
    {
        std::cout << "Filtering project stars (watchers)..." << std::endl;
        std::ofstream w{OutputDir + "/watchers.csv"};
        CSVReader::Parse(InputDir + "/watchers.csv", [&](std::vector<std::string> & row) {
            if (valid_projects.find(std::stoull(row[0])) != valid_projects.end()) {
                w << row[0] << "," // repo id
                  << row[1] << "," // user id
                  << EscapeQuotes(row[2]) << std::endl; // time
                valid_users.insert(std::stoull(row[1]));
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

int main(int argc, char * argv[]) {
    try {
        if (argc != 6)
            throw std::runtime_error{"Invalid number of arguments"};
        std::string lang{argv[1]};
        InputDir = argv[2];
        OutputDir = argv[3];
        std::filesystem::create_directories(OutputDir);
        auto project_commits = GetProjectIds(lang);
        AssignCommitsToProjects(project_commits);
        FilterSmallProjects(project_commits, std::stoull(argv[4]));
        SampleProjects(project_commits, std::stoull(argv[5]));
        FilterProjects(project_commits);
        FilterDataset(project_commits);
        return EXIT_SUCCESS;
    } catch (std::exception const & e) {
        std::cout << "Invalid usage: " << e.what() << std::endl << std::endl;
        std::cout << "./filter lang input_dir output_dir min_commits sample_projects" << std::endl;
        return EXIT_FAILURE;
    }
}





