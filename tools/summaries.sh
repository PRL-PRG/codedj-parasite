#!/bin/bash

./compacter /dejacode/artifact-inputs/full-dataset.csv /dejacode/artifact-inputs/summary-full-pl.csv /dejacode/artifact-inputs/summary-full-p.csv

./compacter /dejacode/artifact-inputs/stars.csv /dejacode/artifact-inputs/summary-stars-pl.csv /dejacode/artifact-inputs/summary-stars-p.csv

./compacter /dejacode/artifact-inputs/mean_changes_in_commits.csv /dejacode/artifact-inputs/summary-changes-pl.csv /dejacode/artifact-inputs/summary-changes-p.csv

./compacter /dejacode/artifact-inputs/experienced_authors.csv /dejacode/artifact-inputs/summary-expa-pl.csv /dejacode/artifact-inputs/summary-expa-p.csv

./compacter /dejacode/artifact-inputs/experienced_authors_ratio.csv /dejacode/artifact-inputs/summary-expr-pl.csv /dejacode/artifact-inputs/summary-expr-p.csv

./compacter /dejacode/artifact-inputs/median_commit_message_sizes.csv /dejacode/artifact-inputs/summary-messages-pl.csv /dejacode/artifact-inputs/summary-messages-p.csv

./compacter /dejacode/artifact-inputs/commits.csv /dejacode/artifact-inputs/summary-commits-pl.csv /dejacode/artifact-inputs/summary-commits-p.csv

./compacter /dejacode/artifact-inputs/all_issues.csv /dejacode/artifact-inputs/summary-issues-pl.csv /dejacode/artifact-inputs/summary-issues-p.csv


