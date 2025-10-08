import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from main import (
    Settings,
    flatten_game,
    game_extractor,
    init_rabbitmq,
    process_games,
    publish_to_rabbitmq,
    save_flattened_game,
)


@pytest.fixture
def tmp_dir(tmp_path: Path):
    d = tmp_path / "games"
    d.mkdir()
    return d


@pytest.fixture
def sample_game():
    return {
        "id": "game_1",
        "begin_at": "2024-01-01T00:00:00Z",
        "map": {"id": 100},
        "match": {
            "league": {"id": 10},
            "serie": {"id": 20, "tier": "a"},
            "tournament": {"id": 30},
        },
        "players": [
            {
                "player": {"id": f"p{i}"},
                "team": {"id": "t1" if i < 5 else "t2"},
                "opponent": {"id": "t2" if i < 5 else "t1"},
                "adr": 100,
                "kast": 70,
                "rating": 1.1,
                "kills": 10,
                "deaths": 5,
                "assists": 2,
                "headshots": 3,
                "flash_assists": 1,
                "first_kills_diff": 1,
                "k_d_diff": 5,
            }
            for i in range(10)
        ],
        "rounds": [
            {
                "round": i,
                "ct": "t1",
                "terrorists": "t2",
                "winner_team": "t1",
                "outcome": "eliminated",
            }
            for i in range(1, 17)
        ],
    }


def test_flatten_game_valid(sample_game):
    result = flatten_game(sample_game)
    assert result, "Flatten game should return data"
    assert all("game_id" in r for r in result)
    assert all("round" in r for r in result)


def test_flatten_game_invalid_missing_id():
    assert flatten_game({}) == []


def test_save_flattened_game(tmp_dir):
    data = [{"a": 1}]
    file_path = tmp_dir / "test"
    save_flattened_game(file_path, "g1", data)
    output = json.loads((file_path / "g1.json").read_text())
    assert output == data


def test_game_extractor_reads_json(tmp_dir):
    f = tmp_dir / "sample.json"
    f.write_text(json.dumps({"a": 1}))
    files = list(game_extractor(tmp_dir))
    assert len(files) == 1
    assert files[0]["a"] == 1


def test_game_extractor_handles_invalid_json(tmp_dir, caplog):
    f = tmp_dir / "broken.json"
    f.write_text("{ invalid json }")
    list(game_extractor(tmp_dir))
    assert "Skipping" in caplog.text


@patch("pika.BlockingConnection")
def test_publish_to_rabbitmq(mock_conn):
    settings = Settings()
    mock_channel = MagicMock()
    mock_conn.return_value.channel.return_value = mock_channel

    conn, ch = init_rabbitmq(settings)
    publish_to_rabbitmq(ch, settings)

    mock_channel.basic_publish.assert_called_once()
    mock_channel.exchange_declare.assert_called()
    mock_channel.queue_bind.asgitsert_called()
    conn.close()


@patch("main.flatten_game", return_value=[{"game_id": "g1"}])
@patch("main.save_flattened_game")
@patch("main.game_extractor", return_value=[{"id": "g1"}])
def test_process_games(_, mock_save, __, tmp_dir):
    settings = Settings(GAMES_RAW_DIR=tmp_dir, GAMES_FLATTEN_DIR=tmp_dir)
    games = process_games(settings)
    assert games == ["g1"]
    mock_save.assert_called_once()
