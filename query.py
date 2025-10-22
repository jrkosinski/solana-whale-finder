#!/usr/bin/env python3
"""
find_pumpfun_wallets.py

Basic Helius + RPC script to find wallets that buy tokens from pump.fun program
and later sell them on common DEX programs (Raydium / Jupiter). 

Outputs a CSV 'pumpfun_wallet_leaderboard.csv' with simple counts and timestamps.

Notes:
- This is a starting point: extend with USD price joins or FIFO lot-matching for real PnL.
- Be mindful of rate limits (use API key and respect request pacing).
"""

import os
import time
import csv
import requests
from dateutil import parser as dateparse
from datetime import datetime, timedelta
from tqdm import tqdm

# ---------- Configuration ----------
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")  # set this in your env
if not HELIUS_API_KEY:
    raise SystemExit("Please set HELIUS_API_KEY environment variable before running.")

RPC_URL = f"https://rpc.helius.xyz/?api-key={HELIUS_API_KEY}"
# pump.fun program ID (common public ID observed in explorers / docs)
PUMPFUN_PROGRAMS = {
    # common pump.fun program id (may change; you can add more if needed)
    "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
    # older/alternate: 'pumpfun11111111111111111111111111111111' (if you see it in your data)
}

# Known DEX program IDs to treat as "sell" destinations (expand if desired)
DEX_PROGRAM_IDS = {
    # Jupiter router (common)
    "JUP6LkbZ9K1jkc2r9nuQGY4JQ6dZRWpDn5XyFCv5GqvH",
    # Raydium AMM program (example program id; expand with the exact AMM id(s))
    "4ckmDgGzLy8NKY7x3Y1CEi9QJzLLn8hpA1ro3kK5RkKt",
    # Orca / Whirlpool may have different IDs; add as you discover
}

# Time windows / limits
RECENT_WINDOW_DAYS = 7            # how many days back to search pump.fun txs
LOOKAHEAD_SELL_WINDOW_HOURS = 48  # after a buy, look this far ahead for sells
MAX_SIGS_PER_REQUEST = 1000       # getSignaturesForAddress limit (RPC may limit)
SLEEP_BETWEEN_RPC = 0.15          # seconds (pace requests to avoid throttling)

OUTPUT_CSV = "pumpfun_wallet_leaderboard.csv"

# ---------- Helpers ----------
def rpc_request(method, params):
    payload = {
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    }
    r = requests.post(RPC_URL, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"RPC error: {data['error']}")
    return data.get("result")

def get_signatures_for_address(address, before=None, limit=1000):
    """
    Wrapper for getSignaturesForAddress via Helius RPC endpoint.
    Returns array of signature objects with 'signature' and 'blockTime' fields.
    """
    # params: [address, {limit: n, before: "..."}]
    opts = {"limit": limit}
    if before:
        opts["before"] = before
    return rpc_request("getSignaturesForAddress", [address, opts])

def get_transaction(sig):
    """
    getTransaction with jsonParsed encoding via RPC.
    """
    # params: [signature, {"encoding":"jsonParsed", "commitment":"confirmed"}]
    return rpc_request("getTransaction", [sig, {"encoding": "jsonParsed", "commitment": "confirmed", "maxSupportedTransactionVersion": 0}])

def parse_block_time(tx):
    # tx['blockTime'] may exist at top-level result depending on provider; also fallback to meta
    t = tx.get("blockTime") or tx.get("meta", {}).get("blockTime")
    if t:
        return datetime.utcfromtimestamp(t)
    # fallback: parse from tx['slot'] isn't direct — return None
    return None

# ---------- Core logic ----------
def find_pumpfun_txs(program_ids, days_back=7):
    """
    Return list of transaction signatures that invoke any of program_ids in the time window.
    Approach: scan signatures for each program account (program is an address we can get signatures for).
    """
    results = []
    cutoff = datetime.utcnow() - timedelta(days=days_back)
    cutoff_unix = int(cutoff.timestamp())
    print(f"Finding transactions invoking programs {program_ids} since {cutoff.isoformat()}...")

    for prog in program_ids:
        # iterate pages of signatures
        before = None
        while True:
            print(f"Fetching signatures for program {prog}, before={before}...")
            sigs = get_signatures_for_address(prog, before=before, limit=1000)
            time.sleep(SLEEP_BETWEEN_RPC)

            if not sigs:
                break
            for s in sigs:
                bt = s.get("blockTime")
                if not bt or bt < cutoff_unix:
                    # older than cutoff: stop processing this program's signature list
                    # because results are in descending time order
                    sigs = []
                    break
                results.append({"signature": s["signature"], "blockTime": bt, "program": prog})
                print(f"  Found tx {s['signature']} at blockTime {bt} invoking {prog}")
                print(f"  Found tx {s['signature']} at blockTime {bt} invoking {len(results)}")

            if not sigs or len(sigs) < 1000:
                break
            if (len(results) >= 100):
                break
            before = sigs[-1]["signature"]
    # dedupe by signature
    uniq = {}

    print(f"Found {len(results)} total invocations of pump.fun programs; deduplicating...")
    for r in results:
        uniq[r["signature"]] = r
    return list(uniq.values())

def extract_token_balance_changes(parsed_tx):
    """
    Using jsonParsed transaction structure, read preTokenBalances/postTokenBalances
    and compute delta per owner token account to get (owner, mint, delta_amount).
    Returns list of dicts with keys: owner, mint, delta (decimal string), decimals
    """
    changes = []
    meta = parsed_tx.get("meta") or {}
    pre = meta.get("preTokenBalances") or []
    post = meta.get("postTokenBalances") or []

    # build maps by (accountIndex or owner + mint)
    def key(tb):
        # tb has 'owner' and 'mint'
        return (tb.get("owner"), tb.get("mint"))

    pre_map = {key(tb): tb for tb in pre}
    post_map = {key(tb): tb for tb in post}

    # union keys
    all_keys = set(pre_map.keys()) | set(post_map.keys())
    for k in all_keys:
        owner, mint = k
        pre_tb = pre_map.get(k)
        post_tb = post_map.get(k)
        pre_amt = int(pre_tb["uiTokenAmount"]["amount"]) if pre_tb else 0
        post_amt = int(post_tb["uiTokenAmount"]["amount"]) if post_tb else 0
        decimals = (post_tb or pre_tb or {}).get("uiTokenAmount", {}).get("decimals", 0)
        delta_raw = post_amt - pre_amt
        # convert raw -> pretty amount
        delta = delta_raw / (10 ** decimals) if decimals is not None else delta_raw
        changes.append({"owner": owner, "mint": mint, "delta": delta, "decimals": decimals})
    return changes

def is_program_invoked(parsed_tx, program_ids):
    """
    Check transaction message instructions to see if one of program_ids was invoked.
    parsed_tx is the 'result' returned from getTransaction with jsonParsed format.
    """
    msg = parsed_tx.get("transaction", {}).get("message", {})
    instructions = msg.get("instructions", []) or []
    for ins in instructions:
        # ins may be parsed form; programId or programIdIndex might be present
        pid = ins.get("programId") or ins.get("program")
        if pid and pid in program_ids:
            return True
        # sometimes programId is nested differently in "parsed" structs:
        if ins.get("programId") and ins["programId"] in program_ids:
            return True
    return False

# ---------- Main scanning + matching ----------
def build_leaderboard():
    print("Scanning pump.fun program txs...")
    pump_txs = find_pumpfun_txs(PUMPFUN_PROGRAMS, days_back=RECENT_WINDOW_DAYS)
    print(f"Found {len(pump_txs)} tx signatures referencing pump.fun programs within last {RECENT_WINDOW_DAYS} days.")

    # map wallet -> per-token buy timestamps and counts
    buys = {}   # { wallet: { mint: [ {sig, time, amount} ] } }
    sells = {}  # { wallet: { mint: [ {sig, time, amount} ] } }

    # fetch each tx and look for token balance increases to owners (buys)
    for rec in tqdm(pump_txs):
        sig = rec["signature"]
        try:
            tx = get_transaction(sig)
            time.sleep(SLEEP_BETWEEN_RPC)
        except Exception as e:
            print("Error fetching tx", sig, e)
            continue
        if not tx:
            continue
        # get block_time
        bt = parse_block_time(tx) or datetime.utcnow()
        # parse token balance changes
        changes = extract_token_balance_changes(tx)
        for ch in changes:
            # buyer = owner who gained tokens (positive delta)
            if ch["delta"] > 0 and ch["owner"]:
                wallet = ch["owner"]
                mint = ch["mint"]
                buys.setdefault(wallet, {}).setdefault(mint, []).append({
                    "sig": sig, "time": bt, "amount": ch["delta"]
                })

    print(f"Identified buys from {len(buys)} unique wallets (received tokens from pump.fun txs).")

    # For each buyer wallet, fetch their recent signatures and look for sells via DEX programs.
    for wallet in tqdm(list(buys.keys()), desc="Finding sells for buyers"):
        # get recent signatures for wallet (limit pages if many)
        try:
            sigs = get_signatures_for_address(wallet, limit=1000) or []
            time.sleep(SLEEP_BETWEEN_RPC)
        except Exception as e:
            print("error fetching signatures for wallet", wallet, e)
            continue
        # iterate signatures and look for transactions invoking known DEX program ids
        for s in sigs:
            sig = s["signature"]
            try:
                tx = get_transaction(sig)
                time.sleep(SLEEP_BETWEEN_RPC)
            except Exception as e:
                print("error fetching tx", sig, e)
                continue
            if not tx:
                continue
            # detect if any of DEX_PROGRAM_IDS were invoked
            if not is_program_invoked(tx, DEX_PROGRAM_IDS):
                continue
            # get token deltas for this tx
            changes = extract_token_balance_changes(tx)
            # look for negative deltas (tokens leaving the wallet)
            for ch in changes:
                if ch["owner"] == wallet and ch["delta"] < 0:
                    mint = ch["mint"]
                    sells.setdefault(wallet, {}).setdefault(mint, []).append({
                        "sig": sig, "time": parse_block_time(tx) or datetime.utcnow(), "amount": abs(ch["delta"])
                    })

    # Create leaderboard rows
    rows = []
    for wallet, token_map in buys.items():
        unique_tokens = set(token_map.keys())
        total_buys = sum(len(lst) for lst in token_map.values())
        total_sells = 0
        first_buy_time = None
        first_sell_time = None
        for mint, blist in token_map.items():
            for b in blist:
                t = b["time"]
                if not first_buy_time or t < first_buy_time:
                    first_buy_time = t
            sold_list = sells.get(wallet, {}).get(mint, [])
            total_sells += len(sold_list)
            for s in sold_list:
                t = s["time"]
                if not first_sell_time or t < first_sell_time:
                    first_sell_time = t

        rows.append({
            "wallet": wallet,
            "unique_tokens_traded": len(unique_tokens),
            "buys_count": total_buys,
            "sells_count": total_sells,
            "first_buy_time": first_buy_time.isoformat() if first_buy_time else "",
            "first_sell_time": first_sell_time.isoformat() if first_sell_time else ""
        })

    # sort by buys_count desc then sells_count desc
    rows.sort(key=lambda r: (r["sells_count"], r["buys_count"]), reverse=True)

    # write CSV
    with open(OUTPUT_CSV, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["wallet", "unique_tokens_traded", "buys_count", "sells_count", "first_buy_time", "first_sell_time"])
        writer.writeheader()
        for r in rows:
            writer.writerow(r)

    print(f"Done. Wrote {len(rows)} wallet rows to {OUTPUT_CSV}.")


if __name__ == "__main__":
    build_leaderboard()
