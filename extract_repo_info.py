import argparse
import csv
import logging
import requests
import sys
import time
from time import strftime, localtime
from typing import Any, Dict, List, NamedTuple, Optional, Set, Union
from alive_progress import alive_bar

API_HOST = "https://api.github.com"

HEADER_RATELIMIT_LIMIT = "x-ratelimit-limit"
HEADER_RATELIMIT_REMAINING = "x-ratelimit-remaining"
HEADER_RATELIMIT_RESET = "x-ratelimit-reset"

MAX_NB_RETRIES = 20
RETRY_SLEEP_DEFAULT = 5
MIN_RATE_LIMIT_REMAINING = 10


def get_repos(
    organization: str, request_headers: Optional[Dict[str, str]] = None
) -> Optional[List[Dict[str, Any]]]:
    return run_request(
        f"{API_HOST}/orgs/{organization}/repos?sort=full_name&per_page=100&page=1&type=all",
        request_headers,
        is_paginated=True,
    )


def get_contributors(
    repo_name: str, request_headers: Optional[Dict[str, str]] = None
) -> Optional[List[Dict[str, Any]]]:
    return run_request(
        f"{API_HOST}/repos/{repo_name}/contributors?sort=full_name&per_page=100&page=1",
        request_headers,
        is_paginated=True,
    )


def get_all_teams(
    organization: str, request_headers: Optional[Dict[str, str]] = None
) -> Optional[List[Dict[str, Any]]]:
    return run_request(
        f"{API_HOST}/orgs/{organization}/teams?per_page=100&page=1",
        request_headers,
        is_paginated=True,
    )


def get_team_members(
    organization: str, team: str, request_headers: Optional[Dict[str, str]] = None
) -> Optional[List[Dict[str, Any]]]:
    return run_request(
        f"{API_HOST}/orgs/{organization}/teams/{team}/members?per_page=100&page=1",
        request_headers,
        is_paginated=True,
    )


def get_participation_stats(
    repo_name: str, request_headers: Optional[Dict[str, str]] = None
) -> Optional[Dict[str, Any]]:
    return run_request(
        f"{API_HOST}/repos/{repo_name}/stats/participation",
        request_headers,
        is_paginated=False,
    )


def run_request(
    url: str,
    request_headers: Optional[Dict[str, str]],
    is_paginated: bool,
):
    failure_count = 0
    failure_status_codes = set()  # type: Set[int]
    while failure_count < MAX_NB_RETRIES:
        raw_results = requests.get(url, headers=request_headers)

        if raw_results.status_code == 200:
            logging.info(f"Request to %s succeeded", url)
            # Continue to recursively collect more results, if applicable.
            return _process_successful_result(
                raw_results, request_headers, is_paginated
            )

        if raw_results.status_code == 204:
            logging.info(f"No data for %s", url)
            return None

        logging.info(f"Retrying %s", url)

        failure_status_codes.add(raw_results.status_code)
        failure_count += 1

        time.sleep(_sleep_duration(raw_results))

    logging.error(
        f"URL: {url}; Status Codes: {', '.join(str(sc) for sc in failure_status_codes)}"
    )
    return None


def _process_successful_result(
    successful_response: requests.Response,
    request_headers: Optional[Dict[str, str]],
    is_paginated: bool,
) -> Optional[Union[List[Dict[str, Any]], Dict[str, Any]]]:
    """For a successful API response, continue to paginate through any remaining results and
    combine all the results into one final result set.
    """
    assert successful_response.status_code == 200
    json_results = successful_response.json()
    if is_paginated and "next" in successful_response.links:
        # All paginated endpoints should return lists. The total result is the concatenation
        # of the lists from all the endpoints.
        assert isinstance(json_results, list)
        additional_results = run_request(
            successful_response.links["next"]["url"],
            request_headers,
            is_paginated=is_paginated,
        )
        if additional_results:
            assert isinstance(additional_results, list)
            json_results.extend(additional_results)
    return json_results


def _sleep_duration(failed_response: requests.Response) -> int:
    """Determine how long the process should wait before retrying a failed response."""
    rate_limit_max = int(failed_response.headers[HEADER_RATELIMIT_LIMIT])
    rate_limit_remaining = int(failed_response.headers[HEADER_RATELIMIT_REMAINING])
    rate_limit_reset_date = int(failed_response.headers[HEADER_RATELIMIT_RESET])
    now = int(time.time())

    logging.info(
        f"Rate Limit (%s / %s) [%s]",
        rate_limit_remaining,
        rate_limit_max,
        rate_limit_reset_date,
    )

    if rate_limit_remaining < MIN_RATE_LIMIT_REMAINING:
        sleep_time = rate_limit_reset_date - now
        readable_time = strftime("%Y-%m-%d %H:%M:%S", localtime(rate_limit_reset_date))
        logging.warning(
            f"Waiting until rate limiting resets at %s (in %s seconds)",
            readable_time,
            sleep_time,
        )
        return sleep_time
    return RETRY_SLEEP_DEFAULT


def get_logins_by_team_slug(
    owner: str,
    request_headers: Optional[Dict[str, str]],
) -> Dict[str, Set[str]]:
    all_teams = get_all_teams(owner, request_headers) or []
    members_by_team_slug = {
        team["slug"]: get_team_members(owner, team["slug"], request_headers) or []
        for team in all_teams
    }
    return {
        team_slug: {member["login"] for member in members}
        for team_slug, members in members_by_team_slug.items()
    }


class RepoRow(NamedTuple):
    repo_name: str
    repo_id: int
    repo_size: int
    last_3_weeks_commit_count: int
    last_52_weeks_commit_count: int
    contributor_count: int
    contributor_handles: str
    team_names: str


def build_repo_row(
    repo: Dict[str, Any],
    repo_contributors: List[Dict[str, Any]],
    repo_stats: Optional[Dict[str, Any]],
    member_logins_by_team_slug: Dict[str, Set[str]],
) -> RepoRow:

    teams = set()
    repo_contributor_logins = {
        contributor["login"] for contributor in repo_contributors
    }
    for team_slug, team_member_logins in member_logins_by_team_slug.items():
        if repo_contributor_logins & team_member_logins:
            teams.add(team_slug)

    return RepoRow(
        repo_name=repo["name"],
        repo_id=repo["id"],
        repo_size=repo["size"],
        last_3_weeks_commit_count=sum(repo_stats["all"][-3:]) if repo_stats else 0,
        last_52_weeks_commit_count=sum(repo_stats["all"][-52:]) if repo_stats else 0,
        contributor_count=len(repo_contributor_logins),
        contributor_handles=", ".join(sorted(repo_contributor_logins)),
        team_names=", ".join(sorted(teams)),
    )


def start_repo_csv(filepath: str) -> bool:
    """Tries to start a new CSV file at the given path, and write the CSV header row.

    Fails and returns False if a file already exists at the given path.
    """
    try:
        with open(filepath, "x", newline="") as csvfile:
            field_names = RepoRow._fields
            repo_writer = csv.DictWriter(
                csvfile, dialect="excel", fieldnames=field_names
            )
            repo_writer.writeheader()
    except FileExistsError:
        return False
    return True


def write_repo_row(filepath: str, row: RepoRow) -> None:
    """Write a single row to an existing csv."""
    with open(filepath, "a", newline="") as csvfile:
        field_names = RepoRow._fields
        repo_writer = csv.DictWriter(csvfile, dialect="excel", fieldnames=field_names)
        repo_writer.writerow(row._asdict())


def get_repo_ids_from_csv(filepath: str) -> Optional[Set[int]]:
    try:
        with open(filepath) as csvfile:
            repo_reader = csv.DictReader(csvfile, dialect="excel")
            return {int(row["repo_id"]) for row in repo_reader if row["repo_id"]}
    except FileNotFoundError:
        return None


parser = argparse.ArgumentParser(
    description="Generate CSVs with summary data from Github API."
)
parser.add_argument(
    "-o",
    "--owner",
    dest="owner",
    required=True,
    help="Github owner from which to analyze repos",
    type=str,
)
parser.add_argument(
    "-t",
    "--token",
    dest="token",
    required=False,
    help="Github personal access token to authenticate API requests",
    type=str,
    default=None,
)
parser.add_argument(
    "-f" "--csv-filepath",
    dest="filepath",
    required=True,
    help="file path for generated csv of contributor stats per repo-team combination",
    type=str,
)
parser.add_argument(
    "--append-only",
    dest="append_only",
    action=argparse.BooleanOptionalAction,
    default=False,
)


args = parser.parse_args()

# In --append-only mode, we will check which repositories are already included in the CSV and then
# only query the API and add additional rows for the ones that are missing.
# When not in --append-only mode, it's required that the CSV file does not already exist. The file
# is created and rows added to it for all repositories.
previously_processed_repo_ids = set()  # type: Set[int]
if args.append_only:
    ids_from_csv = get_repo_ids_from_csv(args.filepath)
    if ids_from_csv is None:
        sys.exit(
            "The given file does not already exist. Do not run in --append-only mode if you want "
            "to create a new CSV."
        )
    else:
        previously_processed_repo_ids = ids_from_csv
else:
    file_exists = not start_repo_csv(args.filepath)
    if file_exists:
        sys.exit(
            "The given file already exists. Do you want to use --append-only mode?"
        )

headers = None
if args.token:
    headers = {
        "Authorization": f"Bearer {args.token}",
        "Accept": "application/vnd.github+json",
    }

# With every run of this script, we use the API to enumerate all the repositories and all the
# mappings of logins to teams.
logging.warning("Fetching Repos")
repos = get_repos(args.owner, headers) or []
logging.warning("Fetching contributors")
member_logins_by_team_slug = get_logins_by_team_slug(args.owner, headers)

with alive_bar(len(repos)) as bar:
    for repo in repos:
        if repo["id"] in previously_processed_repo_ids:
            bar(skipped=True)
            continue
        repo_contributors = get_contributors(repo["full_name"], headers) or []
        repo_stats = get_participation_stats(repo["full_name"], headers)
        repo_row = build_repo_row(
            repo, repo_contributors, repo_stats, member_logins_by_team_slug
        )
        write_repo_row(filepath=args.filepath, row=repo_row)
        bar()
