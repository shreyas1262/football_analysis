import logging
import os
import time

import requests

logger = logging.getLogger("football_api_client")

BASE_URL = "https://api.football-data.org/v4"
RATE_LIMIT_SLEEP = 6  # free tier: 10 requests/minute


class FootballAPIClient:
    def __init__(self):
        api_key = os.environ["FOOTBALL_API_KEY"]
        self.session = requests.Session()
        self.session.headers.update({"X-Auth-Token": api_key})

    def _get(self, path, params=None):
        url = f"{BASE_URL}{path}"
        logger.info("GET %s params=%s", url, params)
        response = self.session.get(url, params=params, timeout=30)
        response.raise_for_status()
        time.sleep(RATE_LIMIT_SLEEP)
        return response.json()

    def get_competitions(self):
        data = self._get("/competitions")
        return data["competitions"]

    def get_teams(self, competition_code, season=None):
        params = {}
        if season is not None:
            params["season"] = season
        data = self._get(f"/competitions/{competition_code}/teams", params=params or None)
        competition_id = data["competition"]["id"]
        for team in data["teams"]:
            team["_competition_id"] = competition_id
        return data["teams"]

    def get_matches(self, competition_code, season=None, status=None):
        params = {}
        if season is not None:
            params["season"] = season
        if status is not None:
            params["status"] = status
        data = self._get(f"/competitions/{competition_code}/matches", params=params or None)
        return data["matches"]

    def get_standings(self, competition_code, season=None):
        params = {}
        if season is not None:
            params["season"] = season
        data = self._get(f"/competitions/{competition_code}/standings", params=params or None)
        competition_id = data["competition"]["id"]
        season_id = data["season"]["id"]
        flat = []
        for table in data["standings"]:
            stage = table.get("stage")
            standing_type = table.get("type")
            for entry in table["table"]:
                entry["_competition_id"] = competition_id
                entry["_season_id"] = season_id
                entry["_stage"] = stage
                entry["_type"] = standing_type
                flat.append(entry)
        return flat

    def get_squad(self, team_id):
        data = self._get(f"/teams/{team_id}")
        squad = data.get("squad", [])
        for member in squad:
            member["_team_id"] = team_id
        return squad
