"""Export the Silence Report UI and API payloads as a fully static site.

Run as:
    PYTHONPATH=src python -m unpicked_intent.export_static --out dist

Uses FastAPI's TestClient against unpicked_intent.api.main:app so the exported
JSON matches the live API responses exactly. Never commits output (dist/ is
gitignored and blocked by the license-boundary pre-push hook).
"""

from __future__ import annotations

import argparse
import re
import shutil
import sys
from pathlib import Path
from urllib.parse import quote

from fastapi.testclient import TestClient

import unpicked_intent.api.main as api_main


def _get_200_json_bytes(client: TestClient, path: str) -> bytes:
    """Issue a GET request against the FastAPI app and fail loudly if status != 200."""
    resp = client.get(path)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Export request GET {path} failed with HTTP {resp.status_code}: {resp.text}"
        )
    # Validate JSON payload is well-formed before writing exact bytes.
    resp.json()
    return resp.content


def _prepare_static_index_html(html: str) -> str:
    """Inject data-static="1" on <html> and rewrite asset paths to relative URLs."""
    match = re.search(r"<html\b([^>]*)>", html, flags=re.IGNORECASE)
    if not match:
        raise RuntimeError("index.html is missing an <html> tag")

    attrs = match.group(1)
    if not re.search(r"\bdata-static\s*=", attrs, flags=re.IGNORECASE):
        new_tag = f'<html{attrs} data-static="1">'
        html = html[: match.start()] + new_tag + html[match.end() :]

    html = html.replace('href="/styles.css"', 'href="styles.css"')
    html = html.replace('src="/app.js"', 'src="app.js"')
    return html


def export_static(out_dir: str | Path) -> dict[str, int | str]:
    """Export static web assets and API JSON payloads into out_dir."""
    db_path = api_main.DB_PATH
    if not db_path.is_file():
        raise FileNotFoundError(
            f"Scouting database not found at {db_path}. "
            "Run the build, baseline, and signals pipeline before exporting."
        )

    web_dir = api_main.WEB_DIR
    if not web_dir.is_dir():
        raise FileNotFoundError(f"Web assets directory not found at {web_dir}")

    # Fetch and validate all API payloads before writing to disk.
    client = TestClient(api_main.app, raise_server_exceptions=True)

    teams_resp = client.get("/teams")
    if teams_resp.status_code != 200:
        raise RuntimeError(
            f"Export request GET /teams failed with HTTP {teams_resp.status_code}: "
            f"{teams_resp.text}"
        )
    teams_data = teams_resp.json()
    teams_bytes = teams_resp.content

    silence_payloads: list[tuple[str, bytes]] = []
    for team in teams_data:
        team_id = str(team["team_id"])
        encoded_id = quote(team_id, safe="")
        payload = _get_200_json_bytes(client, f"/silence/{encoded_id}")
        silence_payloads.append((team_id, payload))

    self_scout_bytes = _get_200_json_bytes(client, "/self-scout")
    meta_bytes = _get_200_json_bytes(client, "/meta")

    # Fetch Draft Replay payloads for every target team (5 most recent complete games each).
    replay_games_payloads: list[tuple[str, bytes]] = []
    replay_detail_payloads: list[tuple[str, str, bytes]] = []
    for target_id in api_main.TARGET_TEAMS:
        tid = str(target_id)
        encoded_tid = quote(tid, safe="")
        games_resp = client.get(f"/replay/{encoded_tid}/games")
        if games_resp.status_code != 200:
            raise RuntimeError(
                f"Export request GET /replay/{encoded_tid}/games failed with HTTP "
                f"{games_resp.status_code}: {games_resp.text}"
            )
        games_data = games_resp.json()
        replay_games_payloads.append((tid, games_resp.content))

        for game in games_data.get("games", []):
            gid = str(game["game_id"])
            encoded_gid = quote(gid, safe="")
            detail_bytes = _get_200_json_bytes(
                client, f"/replay/{encoded_tid}/{encoded_gid}"
            )
            replay_detail_payloads.append((tid, gid, detail_bytes))

    out_path = Path(out_dir)
    data_dir = out_path / "data"
    silence_dir = data_dir / "silence"
    replay_dir = data_dir / "replay"

    out_path.mkdir(parents=True, exist_ok=True)
    if data_dir.exists():
        shutil.rmtree(data_dir)
    silence_dir.mkdir(parents=True, exist_ok=True)
    replay_dir.mkdir(parents=True, exist_ok=True)

    (data_dir / "teams.json").write_bytes(teams_bytes)
    (data_dir / "meta.json").write_bytes(meta_bytes)
    for team_id, payload in silence_payloads:
        (silence_dir / f"{team_id}.json").write_bytes(payload)
    (data_dir / "self-scout.json").write_bytes(self_scout_bytes)

    for tid, games_bytes in replay_games_payloads:
        team_replay_dir = replay_dir / tid
        team_replay_dir.mkdir(parents=True, exist_ok=True)
        (team_replay_dir / "games.json").write_bytes(games_bytes)
    for tid, gid, detail_bytes in replay_detail_payloads:
        team_replay_dir = replay_dir / tid
        team_replay_dir.mkdir(parents=True, exist_ok=True)
        (team_replay_dir / f"{gid}.json").write_bytes(detail_bytes)

    # Copy web assets into out_path, setting data-static="1" only on dist/index.html.
    for src_item in sorted(web_dir.iterdir()):
        dst_item = out_path / src_item.name
        if src_item.is_dir():
            if dst_item.exists():
                shutil.rmtree(dst_item)
            shutil.copytree(src_item, dst_item)
        elif src_item.name == "index.html":
            raw_html = src_item.read_text(encoding="utf-8")
            static_html = _prepare_static_index_html(raw_html)
            dst_item.write_text(static_html, encoding="utf-8")
        else:
            shutil.copy2(src_item, dst_item)

    return {
        "out_dir": str(out_path),
        "teams": len(silence_payloads),
        "silence_files": len(silence_payloads),
        "replay_teams": len(replay_games_payloads),
        "replay_games": len(replay_detail_payloads),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Export the Unpicked Intent Silence Report as a static site."
    )
    parser.add_argument(
        "--out",
        default="dist",
        help="Output directory for the static site (default: dist).",
    )
    args = parser.parse_args(argv)

    try:
        summary = export_static(args.out)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    print(
        f"Exported static site to {summary['out_dir']}: "
        f"{summary['teams']} teams, {summary['silence_files']} silence files, "
        f"{summary['replay_teams']} replay teams ({summary['replay_games']} games), "
        "meta.json, self-scout.json, and web assets."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
