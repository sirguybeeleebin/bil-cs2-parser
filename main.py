import argparse
import json
import logging
import uuid
from collections import defaultdict
from pathlib import Path
from typing import Literal

import pika
from dateutil.parser import parse
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    APP_LOG_LEVEL: Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"] = "INFO"
    RABBITMQ_URL: str = "amqp://guest:guest@localhost/"
    RABBITMQ_EXCHANGE: str = "cs2_events"
    RABBITMQ_QUEUE_NAME: str = "cs2_events_queue"
    RABBITMQ_ROUTING_KEY: str = "all_games_parsed"
    GAMES_RAW_DIR: Path = Path("../bil-cs2-data/games_raw")
    GAMES_FLATTEN_DIR: Path = Path("../bil-cs2-data/games_flatten")


log = logging.getLogger(__name__)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Game Parser Service")
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    return parser.parse_args()


def parse_env_file(env_file: Path) -> Settings:
    return Settings(_env_file=env_file)


def configure_logger(settings: Settings):
    logging.basicConfig(
        level=getattr(logging, settings.APP_LOG_LEVEL.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def init_rabbitmq(settings: Settings):
    params = pika.URLParameters(settings.RABBITMQ_URL)
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.exchange_declare(
        exchange=settings.RABBITMQ_EXCHANGE, exchange_type="direct", durable=True
    )
    channel.queue_declare(queue=settings.RABBITMQ_QUEUE_NAME, durable=True)
    channel.queue_bind(
        exchange=settings.RABBITMQ_EXCHANGE,
        queue=settings.RABBITMQ_QUEUE_NAME,
        routing_key=settings.RABBITMQ_ROUTING_KEY,
    )
    return connection, channel


def publish_to_rabbitmq(channel, settings: Settings):
    event = {
        "event_uuid": str(uuid.uuid4()),
        "event_type": settings.RABBITMQ_ROUTING_KEY,
    }
    channel.basic_publish(
        exchange=settings.RABBITMQ_EXCHANGE,
        routing_key=settings.RABBITMQ_ROUTING_KEY,
        body=json.dumps(event, ensure_ascii=False).encode(),
        properties=pika.BasicProperties(delivery_mode=2),
    )
    log.info("📨 Published event → %s : %s", settings.RABBITMQ_ROUTING_KEY, event)


def game_extractor(path_to_dir: Path):
    json_files = list(path_to_dir.glob("*.json"))
    if not json_files:
        raise FileNotFoundError(f"No JSON files found in {path_to_dir}")
    for file_path in json_files:
        try:
            with file_path.open("r", encoding="utf-8") as f:
                yield json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.warning(f"Skipping {file_path.name}: {e}")


def save_flattened_game(path_to_dir: Path, game_id: str, data: list[dict]):
    path_to_dir.mkdir(parents=True, exist_ok=True)
    file_path = path_to_dir / f"{game_id}.json"
    with file_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2, default=str)
    log.info("💾 Game %s saved (%d records)", game_id, len(data))


def flatten_game(game: dict) -> list[dict]:
    game_id = game.get("id")
    if not game_id:
        return []
    try:
        begin_at = parse(game.get("begin_at"))
    except Exception:
        log.warning("Skipping game %s: invalid begin_at", game_id)
        return []
    map_id = game.get("map", {}).get("id")
    if not map_id:
        return []
    match = game.get("match", {})
    league_id = match.get("league", {}).get("id")
    serie = match.get("serie", {})
    serie_id = serie.get("id")
    serie_tier = {"s": 1, "a": 2, "b": 3, "c": 4, "d": 5}.get(serie.get("tier"))
    tournament_id = match.get("tournament", {}).get("id")
    players = game.get("players", [])
    if len(players) != 10:
        return []
    dd_team_players = defaultdict(list)
    d_player_stat = {}
    d_teams = {}
    for p in players:
        p_id = p.get("player", {}).get("id")
        t_id = p.get("team", {}).get("id")
        opp_id = p.get("opponent", {}).get("id")
        if not all([p_id, t_id, opp_id]):
            continue
        d_player_stat[p_id] = {
            k: p.get(k, 0)
            for k in [
                "adr",
                "kast",
                "rating",
                "kills",
                "deaths",
                "assists",
                "headshots",
                "flash_assists",
                "first_kills_diff",
                "k_d_diff",
            ]
        }
        dd_team_players[t_id].append(p_id)
        d_teams[t_id] = opp_id
    rounds = game.get("rounds", [])
    if len(rounds) < 16 or not rounds or rounds[0].get("round") != 1:
        return []
    games_flatten = []
    for t_id, p_ids in dd_team_players.items():
        if len(set(p_ids)) != 5:
            continue
        t_opp_id = d_teams.get(t_id)
        if not t_opp_id or t_opp_id not in dd_team_players:
            continue
        p_opp_ids = dd_team_players[t_opp_id]
        for p_id in p_ids:
            for p_opp_id in p_opp_ids:
                for rnd in rounds:
                    ct_id = rnd.get("ct")
                    terrorists_id = rnd.get("terrorists")
                    if not all([ct_id, terrorists_id]):
                        continue
                    rnd_number = rnd.get("round")
                    if not rnd_number:
                        continue
                    if ct_id not in [t_id, t_opp_id] or terrorists_id not in [
                        t_id,
                        t_opp_id,
                    ]:
                        continue
                    winner_team = rnd.get("winner_team")
                    if not winner_team or winner_team not in [t_id, t_opp_id]:
                        continue
                    games_flatten.append(
                        {
                            "game_id": game_id,
                            "begin_at": begin_at.isoformat(),
                            "map_id": map_id,
                            "league_id": league_id,
                            "serie_id": serie_id,
                            "serie_tier": serie_tier,
                            "tournament_id": tournament_id,
                            "team_id": t_id,
                            "team_opponent_id": t_opp_id,
                            "player_id": p_id,
                            "player_opponent_id": p_opp_id,
                            **d_player_stat[p_id],
                            "round": int(rnd_number),
                            "is_ct": int(t_id == ct_id),
                            "outcome": {
                                "exploded": 1,
                                "defused": 2,
                                "eliminated": 3,
                                "timeout": 4,
                            }.get(rnd.get("outcome")),
                            "win": int(winner_team == t_id),
                        }
                    )
    return games_flatten


def process_games(settings: Settings) -> list[str]:
    parsed_games = []
    for game in game_extractor(settings.GAMES_RAW_DIR):
        flat_data = flatten_game(game)
        if flat_data:
            game_id = flat_data[0]["game_id"]
            save_flattened_game(settings.GAMES_FLATTEN_DIR, game_id, flat_data)
            parsed_games.append(game_id)
    return parsed_games


def main():
    args = parse_args()
    settings = parse_env_file(args.env_file)
    configure_logger(settings)
    connection, channel = init_rabbitmq(settings)
    try:
        parsed_games = process_games(settings)
        log.info("✅ Parsed %d games.", len(parsed_games))
        publish_to_rabbitmq(channel, settings)
    finally:
        connection.close()
        log.info("🐇 RabbitMQ connection closed.")


if __name__ == "__main__":
    main()
