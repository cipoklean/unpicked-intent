"""FastAPI layer — serves PRECOMPUTED aggregates from derived/ only.

Never expose raw feed rows. Raw data stays on the build machine; what
ships to the hosted demo is the aggregate tables.

Pages:
    /replay/{series}/{game}  — slot-by-slot draft replay with legal pool
    /silence/{team}          — ranked tells + evidence (n >= 4 only)
    /self-scout              — same engine pointed at Cloud9 Kia
"""

from fastapi import FastAPI

app = FastAPI(title="Unpicked Intent Engine")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
