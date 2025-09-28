from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .crawler import UniversityNewsCrawler, CrawlConfig, DEFAULT_SITES

LOG_FORMAT = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="uninews", description="University cooperation-news crawler")
    p.add_argument("--out", help="Output file (.xlsx or .csv). Default: auto timestamp", default=None)
    p.add_argument("--ext", help="Output extension when --out not provided (xlsx|csv)", default="xlsx")
    p.add_argument("--max-per-site", type=int, default=10, help="Max articles per site")
    p.add_argument("--delay-min", type=float, default=1.0, help="Min delay seconds between pages")
    p.add_argument("--delay-max", type=float, default=3.0, help="Max delay seconds between pages")
    p.add_argument("--timeout", type=int, default=10, help="HTTP timeout seconds")
    p.add_argument("--sites-file", type=Path, help="Optional CSV with name,url per line")
    p.add_argument("-v", "--verbose", action="count", default=0, help="-v for INFO, -vv for DEBUG")
    return p


def load_sites_from_csv(path: Path) -> dict[str, str]:
    import csv
    sites: dict[str, str] = {}
    with path.open("r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = (row.get("name") or "").strip()
            url = (row.get("url") or "").strip()
            if name and url:
                sites[name] = url
    if not sites:
        raise ValueError(f"No sites found in {path} (need headers: name,url)")
    return sites


def main() -> None:
    args = build_parser().parse_args()

    # logging
    level = logging.WARNING
    if args.verbose == 1:
        level = logging.INFO
    elif args.verbose >= 2:
        level = logging.DEBUG
    logging.basicConfig(level=level, format=LOG_FORMAT)

    # config
    cfg = CrawlConfig(
        timeout=args.timeout,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        max_per_site=args.max_per_site,
    )

    # sites
    sites = DEFAULT_SITES
    if args.sites_file:
        sites = load_sites_from_csv(args.sites_file)

    crawler = UniversityNewsCrawler(cfg=cfg, sites=sites)
    crawler.crawl()

    # output
    out_path = args.out or crawler.default_output(args.ext)
    saved = crawler.save(out_path)
    df = crawler.to_dataframe()

    print(f"\nSaved {len(df)} rows to: {saved}")
    if not df.empty:
        print("\nPer-university counts:")
        print(df["university"].value_counts())


if __name__ == "__main__":
    main()