# lightning-strikes-data

Public data feed for the **LightningSentry** mobile app (formerly "Lightning Detection App", `Meteoscience1/lightning-detection-app`).

`recent_strikes.json` is regenerated every 10 minutes by `.github/workflows/pipeline.yml`, which runs `cron_pipeline.py`:

- Pulls real-time lightning flash data from **NOAA GOES-16/18 GLM** (Geostationary Lightning Mapper) via public, anonymous AWS S3 buckets — no credentials required.
- Optionally supplements with **EUMETSAT MTG-LI** if `EUMETSAT_CONSUMER_KEY`/`SECRET` repo secrets are set (skipped otherwise).
- Deduplicates and keeps strikes from the last 30 minutes.

This repo is intentionally public and contains **no app source code** — just the generated data file and the fetch script, so the mobile app can read it directly via:

```
https://raw.githubusercontent.com/Meteoscience1/lightning-strikes-data/main/recent_strikes.json
```

(`raw.githubusercontent.com` 404s on private repos for unauthenticated requests — that's the reason this lives in its own public repo instead of inside the private app repo.)
