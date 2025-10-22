#!/usr/bin/env python3
"""
pumpfun_whale_leaderboard.py

Find wallets that buy from pump.fun, later sell on DEXes, compute realized USD PnL using FIFO
matching, and rank wallets by realized profit / win-rate / avg ROI.

Requirements:
  pip install requests python-dateutil tqdm cachetools

Usage:
  export HELIUS_API_KEY="..."
  python pumpfun_whale_leaderboard.py

Notes:
 - Heuristics: buy cost is inferred from SOL or stablecoin outflows in same tx as token inflow.
 - Sell proceeds inferred from SOL/stablecoin inflow when token outflow occurs.
 - Uses Coingecko to get historic SOL prices (per-hour). Cache used to reduce requests.
"""

import os
import time
import csv
import requests
from dateutil import parser as dateparse
from datetime import datetime, timedelta
from tqdm import tqdm
from cachetools import TTLCache

# ---------- Config ----------
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
if not HELIUS_API_KEY:
    raise SystemExit("Please set HELIUS_API_KEY environment variable")

RPC_URL = f"https://rpc.helius.xyz/?api-key={HELIUS_API_KEY}"
# Pump.fun program IDs to watch (add if you discover others)
PUMPFUN_PROGRAMS = {
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    # add alternate pump.fun program IDs here if needed
}

# DEX program IDs (used to detect sells). Extend as needed.
DEX_PROGRAM_IDS = {
    "JUP6LkbZ9K1jkc2r9nuQGY4JQ6dZRWpDn5XyFCv5GqvH",  # Jupiter router
    "4ckmDgGzLy8NKY7x3Y1CEi9QJzLLn8hpA1ro3kK5RkKt",  # Raydium AMM example
    # add additional AMM/router program IDs you encounter
}

RECENT_WINDOW_HOURS = 4
LOOKAHEAD_SELL_WINDOW_HOURS = 4
SLEEP_BETWEEN_RPC = 0.1

OUTPUT_CSV = "pumpfun_whale_leaderboard.csv"

# Cache for coin prices (coingecko) keyed by hour string -> price_usd
price_cache = TTLCache(maxsize=10000, ttl=60 * 60)  # 1 hour cache

# ---------- Helpers: RPC and parsing ----------
def rpc_request(method, params):
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    r = requests.post(RPC_URL, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"RPC error: {data['error']}")
    return data.get("result")

def get_signatures_for_address(address, before=None, limit=1000):
    opts = {"limit": limit}
    if before:
        opts["before"] = before
    return rpc_request("getSignaturesForAddress", [address, opts])

def get_transaction(sig):
    # include maxSupportedTransactionVersion to avoid version errors
    return rpc_request("getTransaction", [sig, {
        "encoding": "jsonParsed",
        "commitment": "confirmed",
        "maxSupportedTransactionVersion": 1
    }])

def parse_block_time(tx):
    t = tx.get("blockTime") or (tx.get("meta") or {}).get("blockTime")
    return datetime.utcfromtimestamp(t) if t else None

def lamports_to_sol(lamports):
    return lamports / 1e9

# ---------- Token & balance delta parsing ----------
def extract_token_balance_changes(parsed_tx):
    """
    Return list of {owner, mint, delta, decimals} for token accounts.
    delta is positive when post > pre (owner gained tokens).
    """
    changes = []
    meta = parsed_tx.get("meta") or {}
    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    def key(tb):
        return (tb.get("owner"), tb.get("mint"))

    pre_map = {key(tb): tb for tb in pre}
    post_map = {key(tb): tb for tb in post}
    all_keys = set(pre_map.keys()) | set(post_map.keys())

    for k in all_keys:
        owner, mint = k
        pre_tb = pre_map.get(k)
        post_tb = post_map.get(k)
        pre_amt_raw = int(pre_tb["uiTokenAmount"]["amount"]) if pre_tb else 0
        post_amt_raw = int(post_tb["uiTokenAmount"]["amount"]) if post_tb else 0
        decimals = (post_tb or pre_tb or {}).get("uiTokenAmount", {}).get("decimals", 0)
        delta_raw = post_amt_raw - pre_amt_raw
        delta = delta_raw / (10 ** decimals) if decimals is not None else delta_raw
        changes.append({"owner": owner, "mint": mint, "delta": delta, "decimals": decimals})
    return changes

def extract_native_balance_delta(parsed_tx):
    """
    Compute SOL delta per account by comparing preBalances/postBalances arrays.
    Returns list of (pubkey_string, delta_sol) tuples — safe for dict().
    """
    meta = parsed_tx.get("meta") or {}
    pre = meta.get("preBalances") or []
    post = meta.get("postBalances") or []

    # account keys may be objects or strings depending on RPC
    accounts = parsed_tx.get("transaction", {}).get("message", {}).get("accountKeys", [])
    result = []

    for idx, acct in enumerate(accounts):
        try:
            pre_bal = pre[idx]
            post_bal = post[idx]
        except (IndexError, TypeError):
            continue

        delta_lamports = post_bal - pre_bal
        delta_sol = lamports_to_sol(delta_lamports)

        # extract pubkey if account is dict
        if isinstance(acct, dict):
            pubkey = acct.get("pubkey")
        else:
            pubkey = acct

        if pubkey:
            result.append((pubkey, delta_sol))

    return result



# ---------- Pricing (Coingecko simple hourly lookup) ----------
def coingecko_sol_price_at(dt: datetime):
    # round to hour
    hour_key = dt.strftime("%Y-%m-%dT%H:00:00Z")
    if hour_key in price_cache:
        return price_cache[hour_key]
    # Coingecko simple: /coins/solana/history or /coins/solana/market_chart/range
    # We'll use market_chart/range for hour-aligned granularity
    # Build unix timestamps
    from_ts = int((dt - timedelta(hours=1)).timestamp())
    to_ts = int((dt + timedelta(hours=1)).timestamp())
    url = f"https://api.coingecko.com/api/v3/coins/solana/market_chart/range?vs_currency=usd&from={from_ts}&to={to_ts}"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()
    # data['prices'] is list of [ms, price]
    prices = data.get("prices", [])
    if not prices:
        raise RuntimeError("No price points returned")
    # find the price closest to dt
    target_ms = int(dt.timestamp() * 1000)
    best = min(prices, key=lambda p: abs(p[0] - target_ms))
    price = float(best[1])
    price_cache[hour_key] = price
    return price

# ---------- FIFO lot matching ----------
class WalletLedger:
    """
    Per-wallet ledger storing buy lots per token mint for FIFO matching.
    buy_lots: dict mint -> list of {amount, cost_usd, time}
    realized_trades: list of {mint, amount, proceed_usd, cost_usd, profit_usd, buy_time, sell_time}
    """
    def __init__(self, wallet):
        self.wallet = wallet
        self.buy_lots = {}    # mint -> list
        self.realized = []    # realized trades

    def add_buy(self, mint, amount, cost_usd, time):
        if amount <= 0:
            return
        self.buy_lots.setdefault(mint, []).append({"amount": amount, "cost_usd": cost_usd, "time": time})

    def match_sell(self, mint, amount_to_sell, proceed_usd, sell_time):
        """
        FIFO match amount_to_sell vs buy lots. Returns aggregated cost_usd, profit_usd.
        If not enough lots exist, match what you can and leave negative unmatched (treated as 0 cost).
        """
        remaining = amount_to_sell
        cost_accum = 0.0
        matched_amount = 0.0
        lots = self.buy_lots.get(mint, [])
        while remaining > 0 and lots:
            lot = lots[0]
            if lot["amount"] <= remaining + 1e-12:
                # consume entire lot
                consumed = lot["amount"]
                cost_accum += lot["cost_usd"]
                matched_amount += consumed
                remaining -= consumed
                lots.pop(0)
            else:
                # consume part of lot
                frac = remaining / lot["amount"]
                cost_part = lot["cost_usd"] * frac
                cost_accum += cost_part
                lot["amount"] -= remaining
                lot["cost_usd"] -= cost_part
                matched_amount += remaining
                remaining = 0.0
        # If remaining > 0, we sold tokens we don't have in ledger (could be transfers). We treat remaining as zero-cost basis  (conservative)
        proceeds_for_matched = proceed_usd * (matched_amount / amount_to_sell) if amount_to_sell>0 else 0.0
        profit = proceeds_for_matched - cost_accum
        # record realized trade if matched_amount > 0
        if matched_amount > 0:
            self.realized.append({
                "mint": mint,
                "amount": matched_amount,
                "proceeds_usd": proceeds_for_matched,
                "cost_usd": cost_accum,
                "profit_usd": profit,
                "sell_time": sell_time
            })
        return {"matched_amount": matched_amount, "cost_usd": cost_accum, "proceeds_usd": proceeds_for_matched, "profit_usd": profit}

# ---------- High-level pipeline ----------
def build_leaderboard():
    print("Scanning pump.fun signatures...")
    pump_txs = find_pumpfun_txs(PUMPFUN_PROGRAMS, hours_back=RECENT_WINDOW_HOURS)
    print(f"pump.fun tx signatures found: {len(pump_txs)}")

    # record buys: wallet -> list of {mint, amount, cost_usd, time}
    buy_records = []  # simple list records; we'll feed into per-wallet ledger
    # Step A: fetch each pump.fun tx and infer buys and their cost (SOL or stablecoin)
    for rec in tqdm(pump_txs, desc="processing pump.fun txs"):
        sig = rec["signature"]
        try:
            tx = get_transaction(sig)
            time.sleep(SLEEP_BETWEEN_RPC)
        except Exception as e:
            # skip problematic txs
            continue
        if not tx:
            continue
        bt = parse_block_time(tx) or datetime.utcnow()
        # token deltas (owner,mint,delta)
        token_changes = extract_token_balance_changes(tx)
        # native SOL deltas per account
        sol_changes = dict(extract_native_balance_delta(tx))
        # Also inspect stablecoin token changes (USDC/USDT) in token_changes: treat increases/decreases
        # Heuristic for each token change where delta > 0: wallet gained tokens -> consider as buy
        for ch in token_changes:
            if ch["delta"] <= 0 or not ch["owner"]:
                continue
            buyer = ch["owner"]
            mint = ch["mint"]
            token_amount = ch["delta"]
            # Try infer cost in USD:
            cost_usd = None
            # 1) if buyer's SOL decreased in same tx, infer SOL cost
            sol_delta = sol_changes.get(buyer, 0.0)
            if sol_delta < 0:
                # buyer spent -sol_delta SOL; get SOL price at that time
                try:
                    sol_price = coingecko_sol_price_at(bt)
                except Exception:
                    sol_price = None
                if sol_price:
                    cost_usd = -sol_delta * sol_price
            # 2) else look for stablecoin deltas (USDC/USDT) in token_changes for that owner
            if cost_usd is None:
                # find any token change for this owner with symbol USDC/USDT where delta < 0
                for other in token_changes:
                    if other["owner"] == buyer and other["delta"] < 0:
                        # fallback: if symbol present and is a stablecoin, use that (we don't have symbol in extract -> you can extend)
                        # We won't assume so here, skip.
                        pass
            # If cost_usd still None, we set as 0 and still record lot (best effort)
            if cost_usd is None:
                cost_usd = 0.0
            buy_records.append({"wallet": buyer, "mint": mint, "amount": token_amount, "cost_usd": cost_usd, "time": bt, "sig": sig})

    # Build per-wallet FIFO ledger
    ledgers = {}  # wallet -> WalletLedger
    for rec in buy_records:
        w = rec["wallet"]
        ledgers.setdefault(w, WalletLedger(w)).add_buy(rec["mint"], rec["amount"], rec["cost_usd"], rec["time"])

    print("Now scanning buyer wallets for DEX sells...")
    # Step B: for each buyer wallet, scan recent signatures and detect sells (DEX programs)
    for wallet in tqdm(list(ledgers.keys()), desc="scan wallets sells"):
        try:
            sigs = get_signatures_for_address(wallet, limit=1000) or []
            time.sleep(SLEEP_BETWEEN_RPC)
        except Exception:
            continue
        # get transactions for these sigs; naive loop
        for s in sigs:
            sig = s["signature"]
            try:
                tx = get_transaction(sig)
                time.sleep(SLEEP_BETWEEN_RPC)
            except Exception:
                continue
            if not tx:
                continue
            # Skip if tx older than lookahead window relative to first buy of wallet
            first_buy_time = None
            # compute earliest buy time recorded (if none, skip)
            ledger = ledgers.get(wallet)
            if not ledger:
                continue
            # determine if this tx invokes a DEX program
            # look through message.instructions for programId
            msg = tx.get("transaction", {}).get("message", {})
            instructions = msg.get("instructions", []) or []
            invoked = False
            for ins in instructions:
                pid = ins.get("programId") or ins.get("program")
                if pid and pid in DEX_PROGRAM_IDS:
                    invoked = True
                    break
            if not invoked:
                continue
            # parse token deltas
            token_changes = extract_token_balance_changes(tx)
            sol_changes = dict(extract_native_balance_delta(tx))
            bt = parse_block_time(tx) or datetime.utcnow()
            # for any token delta where owner == wallet and delta < 0 => sells
            for ch in token_changes:
                if ch["owner"] != wallet or ch["delta"] >= 0:
                    continue
                mint = ch["mint"]
                sell_amount = abs(ch["delta"])
                # infer proceeds_usd from SOL delta or USDC token inflow
                proceeds_usd = None
                # if wallet's SOL increased in this tx, treat as proceeds
                sol_in = sol_changes.get(wallet, 0.0)
                if sol_in > 0:
                    try:
                        sol_price = coingecko_sol_price_at(bt)
                    except Exception:
                        sol_price = None
                    if sol_price:
                        proceeds_usd = sol_in * sol_price
                # else try look for stablecoin token increases in postTokenBalances (left out here)
                if proceeds_usd is None:
                    # fallback: 0 proceeds (will make profit negative if cost > 0)
                    proceeds_usd = 0.0
                # match with ledger
                res = ledger.match_sell(mint, sell_amount, proceeds_usd, bt)
                # note: we ignore unmatched remainder (treated as 0-cost basis)
            # end token_changes loop
    # end wallet loop

    # Aggregate metrics
    rows = []
    for w, ledger in ledgers.items():
        trades = ledger.realized
        trades_count = len(trades)
        if trades_count == 0:
            continue
        total_profit = sum(t["profit_usd"] for t in trades)
        total_cost = sum(t["cost_usd"] for t in trades)
        total_proceeds = sum(t["proceeds_usd"] for t in trades)
        avg_roi = (sum((t["proceeds_usd"] / t["cost_usd"]) if t["cost_usd"]>0 else 0 for t in trades)/trades_count) if trades_count>0 else 0
        win_rate = sum(1 for t in trades if t["profit_usd"]>0) / trades_count
        avg_hold_hours = sum(((t["sell_time"] - t.get("buy_time", t["sell_time"])).total_seconds()/3600) if t.get("buy_time") else 0 for t in trades)/trades_count
        rows.append({
            "wallet": w,
            "trades_count": trades_count,
            "total_profit_usd": total_profit,
            "total_cost_usd": total_cost,
            "total_proceeds_usd": total_proceeds,
            "avg_roi": avg_roi,
            "win_rate": win_rate,
            "avg_hold_hours": avg_hold_hours
        })

    # sort the leaderboard by total_profit_usd desc
    rows.sort(key=lambda r: r["total_profit_usd"], reverse=True)

    # write CSV
    with open(OUTPUT_CSV, "w", newline="") as f:
        fieldnames = ["wallet", "trades_count", "total_profit_usd", "total_cost_usd", "total_proceeds_usd", "avg_roi", "win_rate", "avg_hold_hours"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"Leaderboard written to {OUTPUT_CSV} ({len(rows)} wallets)")

# ---------- Utility: find pump.fun signatures ----------
def find_pumpfun_txs(program_ids, hours_back=7):
    results = []
    cutoff = datetime.utcnow() - timedelta(hours=hours_back)
    cutoff_unix = int(cutoff.timestamp())
    for prog in program_ids:
        before = None
        while True:
            try:
                sigs = get_signatures_for_address(prog, before=before, limit=1000) or []
            except Exception:
                break
            time.sleep(SLEEP_BETWEEN_RPC)
            if not sigs:
                break
            for s in sigs:
                bt = s.get("blockTime")
                if not bt or bt < cutoff_unix:
                    sigs = []
                    break
                results.append({"signature": s["signature"], "blockTime": bt, "program": prog})
            if not sigs or len(sigs) < 1000:
                break
            before = sigs[-1]["signature"]
            print(f"Fetched {len(results)} pump.fun signatures so far out of {len(program_ids)} total...")
            if (len(results) >= int(os.getenv("PUMPFUN_MAX_SIGS", 100000))):
                print("Reached max sigs limit, stopping fetch.")
                break
    # dedupe
    uniq = {}
    for r in results:
        uniq[r["signature"]] = r
    return list(uniq.values())

if __name__ == "__main__":
    build_leaderboard()
