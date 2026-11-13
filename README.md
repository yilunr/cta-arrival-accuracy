# CTA Arrival Prediction Accuracy

How often are the CTA's "arrives in N min" predictions right? This project polls the CTA Train Tracker and Bus Tracker APIs every two minutes for downtown Chicago, works out when each vehicle actually arrived, and scores every prediction made 2–15 minutes ahead as **early**, **on time** or **late**.

**Live results: https://yilunr.github.io/cta-arrival-accuracy/** (refreshed hourly)

## How it works

Built on Databricks Free Edition with a bronze → silver → gold pipeline.

| Layer | What happens |
|---|---|
| Bronze | `bronze` and `train_bronze` poll the CTA APIs (run together by `ingest_all`) and land raw predictions with Auto Loader |
| Silver | `silver` and `train_silver` clean predictions, infer each vehicle's actual arrival (it ends when the vehicle drops out of predictions), and score every prediction |
| Gold | `gold` and `train_gold` build the accuracy, error-distribution, data-quality and rider tables |
| Publish | `publish_site` exports small summaries to `docs/data.json`, which the GitHub Pages site reads |

Scoring: error = actual arrival minus predicted arrival. Trains count as on time within 2 minutes, buses within 3. Settings live in `_common.py`.

Coverage: 46 train stations (98 platforms) in and around the Loop, plus Loop bus stops.

## Repo layout

```
notebooks/   pipeline notebooks (Databricks source format)
docs/        GitHub Pages site (index.html + data.json)
```

## Setup

1. Get a CTA [Train Tracker](https://www.transitchicago.com/developers/traintrackerapply/) key and [Bus Tracker](https://www.transitchicago.com/developers/bustracker/) key.
2. Store them in a Databricks secret scope named `cta` (train key: `train_api_key`).
3. Import `notebooks/` into Databricks and create a job that runs `ingest_all` every 2 minutes, plus an hourly job for silver → gold → `publish_site`.
4. For the public page, add a GitHub token (Contents: read/write on this repo) as secret `cta` / `github_token`, then enable Pages on the `/docs` folder.

Never commit API keys or tokens.

## Data sources

CTA Train Tracker and Bus Tracker APIs, and the City of Chicago Data Portal for station locations. This project is not affiliated with the CTA.

## License

MIT, see [LICENSE](LICENSE).
