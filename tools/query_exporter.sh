#!/bin/bash
./dcd-export /dejacode/dataset /dejacode/query_results/issues.csv /dejacode/artifact_inputs/projects-by-issues.csv
./dcd-export /dejacode/dataset /dejacode/query_results/buggy_issues.csv /dejacode/artifact_inputs/projects-by-buggy-issues.csv
./dcd-export /dejacode/dataset /dejacode/query_results/stars.csv /dejacode/artifact_inputs/projects-by-stars.csv
./dcd-export /dejacode/dataset /dejacode/query_results/commits.csv /dejacode/artifact_inputs/projects-by-commits.csv
./dcd-export /dejacode/dataset /dejacode/query_results/changes_in_commits.csv /dejacode/artifact_inputs/projects-by-changes.csv
./dcd-export /dejacode/dataset /dejacode/query_results/experienced_authors.csv /dejacode/artifact_inputs/projects-experienced-authors.csv
./dcd-export /dejacode/dataset /dejacode/query_results/experienced_authors_ratio.csv /dejacode/artifact_inputs/projects-experienced-authors-ratio.csv
