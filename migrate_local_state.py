"""One-time upload of the existing D: marketplace database to Supabase."""

from cloud_state import snapshot_sqlite


if __name__ == "__main__":
    if snapshot_sqlite():
        print("Marketplace data uploaded successfully.")
    else:
        raise SystemExit("DATABASE_URL is missing or market.db was not found.")
