# Illumina Navigator v0.6 — Local-first Search

## Start (Windows)
`py app.py serve` then open http://127.0.0.1:8765

No dependencies beyond Python 3.10+, no account, no search APIs, no hosted Illumina server. Uses public websites for crawling and must follow their robots rules. **This is NOT a Google-scale index.** An empty search result means no locally indexed document matches.

At startup it tries a small background crawl once per 24 hours with up to 40 documents, 2 workers and a 2-second minimum delay. Failed/unreachable seeds are skipped. You can turn future startup crawling off via ⚙ Settings, or request manual refresh. Seed sites are listed in app.py and can be edited by the user.

## Keep your existing index
Stop the old app, copy its `data` folder into this directory, then run the command above. Do not copy a live DB. Back up data beforehand.

## Limitations
A machine without a prebuilt index cannot search the entire internet instantly. Sites that require JavaScript rendering, robots exclusions, blocked HTTP redirects or login won't index reliably. This version provides a simple consumer UI, not web-scale search coverage. The crawl may be slow; the default small limit is intentional for low-end hardware. The local HTTP server binds to loopback, not a public endpoint.

## Advanced usage
`py app.py crawl https://www.python.org/ --discover --max-pages 100 --per-host 10 --workers 2 --delay 3`
`py app.py stats`
MIT license.

## v0.6.1 fixes
- Non-ASCII URL paths are percent-encoded before HTTP requests.
- Redirects are followed for the same host only, maximum five hops, with robots checks. Cross-host redirects are still intentionally skipped.
- Single-word searches filter unrelated one-off body mentions and boost domain/title matches.
- Manual refresh explains the already-running (HTTP 409) response.
- Existing `data` folder stays compatible. No external search provider was added.
