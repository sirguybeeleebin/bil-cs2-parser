import json
import logging
from pathlib import Path

import pytest

from main import (
    Settings,
    configure_logger,
    flatten_game,
    game_extractor,
    parse_args,
    parse_env_file,
    process_games,
    save_flattened_game,
)


def test_parse_args_defaults(monkeypatch):
    """Test that default env file is .env."""
    monkeypatch.setattr("sys.argv", ["prog"])
    args = parse_args()
    assert args.env_file == Path(".env")


def test_parse_env_file(tmp_path):
    """Ensure environment variables are loaded correctly."""
    env_file = tmp_path / ".env"
    env_file.write_text("APP_LOG_LEVEL=DEBUG\nRABBITMQ_URL=amqp://test\n")
    s = parse_env_file(env_file)
    assert isinstance(s, Settings)
    assert s.APP_LOG_LEVEL == "DEBUG"
    assert "amqp" in s.RABBITMQ_URL


def test_configure_logger_sets_format():
    """Just ensure configure_logger executes without errors."""
    s = Settings(APP_LOG_LEVEL="INFO")
    configure_logger(s)
    log = logging.getLogger("test_logger")
    log.info("hi")  # should not raise any error


def test_game_extractor_reads_json(tmp_path):
    """Game extractor should yield parsed JSON objects."""
    data = {"id": "game1"}
    (tmp_path / "1.json").write_text(json.dumps(data), encoding="utf-8")
    games = list(game_extractor(tmp_path))
    assert games == [data]


def test_game_extractor_raises_when_empty(tmp_path):
    """Should raise FileNotFoundError if no JSON files exist."""
    with pytest.raises(FileNotFoundError):
        list(game_extractor(tmp_path))


def test_save_flattened_game_creates_file(tmp_path):
    """Should create a JSON file with flattened data."""
    data = [{"id": 1}]
    save_flattened_game(tmp_path, "g1", data)
    file = tmp_path / "g1.json"
    assert file.exists()
    assert json.loads(file.read_text(encoding="utf-8")) == data


def test_flatten_game_valid():
    """Flatten valid game data into per-round stats."""
    game = {
        "id": "g1",
        "begin_at": "2024-01-01T00:00:00Z",
        "map": {"id": "m1"},
        "match": {
            "league": {"id": "l1"},
            "tournament": {"id": "t1"},
            "serie": {"id": "s1", "tier": "a"},
        },
        "players": [
            {
                "player": {"id": f"p{i}"},
                "team": {"id": "t1" if i < 5 else "t2"},
                "opponent": {"id": "t2" if i < 5 else "t1"},
            }
            for i in range(10)
        ],
        "rounds": [
            {
                "round": 1,
                "ct": "t1",
                "terrorists": "t2",
                "winner_team": "t1",
                "outcome": "eliminated",
            }
            for _ in range(16)
        ],
    }
    result = flatten_game(game)
    assert isinstance(result, list)
    assert len(result) > 0
    assert all("game_id" in r for r in result)


def test_flatten_game_invalid_data():
    """Invalid data should return an empty list."""
    assert flatten_game({}) == []
    bad = {"id": "g1", "begin_at": "??", "map": {"id": "m"}, "match": {}, "players": []}
    assert flatten_game(bad) == []


def test_process_games_creates_output(tmp_path):
    """Test full ETL pipeline via process_games() without RabbitMQ."""
    data = {
        "id": "g1",
        "begin_at": "2024-01-01T00:00:00Z",
        "map": {"id": "m1"},
        "match": {
            "league": {"id": "l1"},
            "tournament": {"id": "t1"},
            "serie": {"id": "s1", "tier": "a"},
        },
        "players": [
            {
                "player": {"id": f"p{i}"},
                "team": {"id": "t1" if i < 5 else "t2"},
                "opponent": {"id": "t2" if i < 5 else "t1"},
            }
            for i in range(10)
        ],
        "rounds": [
            {
                "round": 1,
                "ct": "t1",
                "terrorists": "t2",
                "winner_team": "t1",
                "outcome": "eliminated",
            }
            for _ in range(16)
        ],
    }

    raw_dir = tmp_path / "raw"
    flat_dir = tmp_path / "flat"
    raw_dir.mkdir()
    (raw_dir / "g1.json").write_text(json.dumps(data), encoding="utf-8")

    s = Settings(GAMES_RAW_DIR=raw_dir, GAMES_FLATTEN_DIR=flat_dir)
    parsed = process_games(s)

    assert parsed == ["g1"]
    output_files = list(flat_dir.glob("*.json"))
    assert len(output_files) == 1

    content = json.loads(output_files[0].read_text(encoding="utf-8"))
    assert content[0]["game_id"] == "g1"
