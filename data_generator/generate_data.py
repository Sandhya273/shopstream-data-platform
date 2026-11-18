"""
Synthetic source-data generator for the ShopStream retail data platform.

Simulates three real-world source systems and writes them in the exact layout
they would land in the S3 raw bucket:

  1. shop_db (PostgreSQL OLTP)  -> AWS DMS CDC files (CSV, Op column I/U/D)
       landing/dms/<table>/year=YYYY/month=MM/<table>_YYYYMM.csv
  2. Payments gateway REST API  -> mock API pages (JSON) consumed by the extractor
       landing/_mock_payments_api/page_NNNN.json
  3. Web/app clickstream        -> Kinesis Firehose style gzipped JSON lines
       landing/clickstream/dt=YYYY-MM-DD/events_YYYYMMDD.json.gz

Deliberate data-quality problems are injected so the DQ framework and
reconciliation have something real to catch:
  * duplicate CDC records, out-of-order updates, hard deletes
  * customers with missing / malformed emails
  * malformed + duplicated clickstream events, late-arriving events
  * payments with amount mismatches and orders with no payment

Usage:
    python data_generator/generate_data.py --out landing --seed 42
"""
from __future__ import annotations

import argparse
import csv
import gzip
import json
import math
import random
import shutil
import uuid
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path

START = datetime(2024, 11, 1)
END = datetime(2026, 10, 31, 23, 59, 59)
CLICKSTREAM_START = datetime(2026, 8, 1)  # web tracking launched Aug 2026

CITIES = [
    ("Hyderabad", "Telangana"), ("Bengaluru", "Karnataka"), ("Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra"), ("Chennai", "Tamil Nadu"), ("Delhi", "Delhi"),
    ("Kolkata", "West Bengal"), ("Ahmedabad", "Gujarat"), ("Jaipur", "Rajasthan"),
    ("Kochi", "Kerala"), ("Visakhapatnam", "Andhra Pradesh"), ("Lucknow", "Uttar Pradesh"),
]
CITY_WEIGHTS = [14, 14, 13, 8, 9, 12, 6, 6, 4, 4, 5, 5]
FIRST = ["Aarav", "Vivaan", "Aditya", "Sai", "Arjun", "Rohan", "Karthik", "Rahul", "Ananya",
         "Diya", "Sandhya", "Priya", "Meera", "Kavya", "Sneha", "Lakshmi", "Neha", "Pooja",
         "Vikram", "Harsha", "Ishaan", "Nikhil", "Divya", "Swathi", "Anjali", "Ravi"]
LAST = ["Reddy", "Sharma", "Rao", "Iyer", "Nair", "Patel", "Gupta", "Kumar", "Singh",
        "Menon", "Joshi", "Das", "Varma", "Naidu", "Chowdary", "Mehta", "Pillai", "Goud"]
CATALOG = {
    "Electronics": {"Mobiles": (8000, 65000), "Audio": (900, 18000), "Laptops": (32000, 140000),
                    "Accessories": (299, 3500)},
    "Home & Kitchen": {"Cookware": (499, 6500), "Appliances": (1800, 32000), "Decor": (299, 4500)},
    "Fashion": {"Men": (399, 4500), "Women": (449, 6500), "Footwear": (699, 7999)},
    "Beauty": {"Skincare": (199, 2499), "Haircare": (149, 1499)},
    "Sports": {"Fitness": (499, 22000), "Outdoor": (699, 9000)},
    "Books": {"Fiction": (199, 899), "Tech": (399, 2499)},
}
BRANDS = ["Nova", "Zenith", "Aurora", "Kaveri", "Orbit", "Lumen", "Vega", "Tara", "Indus", "Peak"]
CHANNELS = ["web", "app", "app", "web", "marketplace"]
PAY_METHODS = ["UPI", "UPI", "UPI", "card", "card", "netbanking", "wallet", "COD", "COD"]
UTM = [("google", "cpc", "festive_sale"), ("google", "organic", None), ("facebook", "paid_social", "diwali_2026"),
       ("instagram", "paid_social", "new_arrivals"), ("email", "newsletter", "weekly_digest"),
       ("direct", "none", None), ("direct", "none", None), ("affiliate", "referral", "cashback_partners")]


def ts(d: datetime) -> str:
    return d.strftime("%Y-%m-%d %H:%M:%S")


def seasonal_weight(d: date) -> float:
    """Growth trend + festive (Oct/Nov) + weekend uplift."""
    months = (d.year - START.year) * 12 + d.month - START.month
    trend = 1.0 + 0.045 * months
    festive = {10: 1.8, 11: 1.5, 12: 1.2, 1: 1.1, 8: 1.15}.get(d.month, 1.0)
    weekend = 1.2 if d.weekday() >= 5 else 1.0
    return trend * festive * weekend


class Generator:
    def __init__(self, out: Path, seed: int, n_customers: int, n_products: int, base_daily_orders: float):
        self.out = out
        self.rng = random.Random(seed)
        self.n_customers = n_customers
        self.n_products = n_products
        self.base_daily_orders = base_daily_orders
        self.cdc = defaultdict(list)  # table -> list of rows (dict)
        self.stats = defaultdict(int)

    # ------------------------------------------------------------------ helpers
    def rand_ts(self, day: date) -> datetime:
        # more orders in the evening
        hour = min(23, max(0, int(self.rng.gauss(19, 4))))
        return datetime(day.year, day.month, day.day, hour, self.rng.randint(0, 59), self.rng.randint(0, 59))

    def add_cdc(self, table: str, op: str, at: datetime, row: dict):
        self.cdc[table].append({"Op": op, "cdc_ts": ts(at), **row})

    # ---------------------------------------------------------------- customers
    def gen_customers(self):
        self.customers = []
        span = (END - START).days
        for cid in range(1, self.n_customers + 1):
            fn, ln = self.rng.choice(FIRST), self.rng.choice(LAST)
            city, state = self.rng.choices(CITIES, CITY_WEIGHTS)[0]
            # signups skewed towards earlier dates
            signup = START + timedelta(days=int(span * self.rng.random() ** 1.6), seconds=self.rng.randint(0, 86399))
            email = f"{fn}.{ln}{cid}@example.com".lower()
            r = self.rng.random()
            if r < 0.012:
                email = None                       # DQ issue: missing email
            elif r < 0.02:
                email = email.replace("@", "_at_")  # DQ issue: malformed email
            row = {
                "customer_id": cid, "first_name": fn, "last_name": ln, "email": email,
                "phone": f"+91{self.rng.randint(6000000000, 9999999999)}",
                "city": city, "state": state, "country": "India",
                "segment": self.rng.choices(["consumer", "consumer", "small_business", "corporate"], [6, 3, 2, 1])[0],
                "signup_ts": ts(signup), "updated_at": ts(signup),
            }
            self.customers.append(row)
            self.add_cdc("customers", "I", signup, row)
            # ~8% relocate later -> SCD2 history in the warehouse
            if self.rng.random() < 0.08:
                moved_at = signup + timedelta(days=self.rng.randint(30, 400))
                if moved_at < END:
                    ncity, nstate = self.rng.choices(CITIES, CITY_WEIGHTS)[0]
                    upd = {**row, "city": ncity, "state": nstate, "updated_at": ts(moved_at)}
                    self.add_cdc("customers", "U", moved_at, upd)
                    self.stats["customer_relocations"] += 1
        self.signup_by_id = {c["customer_id"]: datetime.strptime(c["signup_ts"], "%Y-%m-%d %H:%M:%S") for c in self.customers}

    # ----------------------------------------------------------------- products
    def gen_products(self):
        self.products = []
        cats = [(c, s, rng) for c, subs in CATALOG.items() for s, rng in subs.items()]
        for pid in range(1, self.n_products + 1):
            cat, sub, (lo, hi) = self.rng.choice(cats)
            price = round(math.exp(self.rng.uniform(math.log(lo), math.log(hi))) / 10) * 10 - 1
            margin = self.rng.uniform(0.12, 0.45)
            created = START - timedelta(days=self.rng.randint(1, 365))
            row = {
                "product_id": pid, "sku": f"SKU-{cat[:3].upper()}-{pid:05d}",
                "product_name": f"{self.rng.choice(BRANDS)} {sub} {self.rng.choice(['Pro', 'Lite', 'Max', 'Classic', 'Plus', 'Prime'])} {pid}",
                "category": cat, "sub_category": sub, "brand": self.rng.choice(BRANDS),
                "unit_price": price, "unit_cost": round(price * (1 - margin), 2),
                "is_active": True, "updated_at": ts(created),
            }
            self.products.append(row)
            self.add_cdc("products", "I", created, row)
            # ~15% get a price revision
            if self.rng.random() < 0.15:
                at = START + timedelta(days=self.rng.randint(60, 600))
                new_price = round(price * self.rng.uniform(0.85, 1.12) / 10) * 10 - 1
                row = {**row, "unit_price": new_price, "updated_at": ts(at)}
                self.add_cdc("products", "U", at, row)
        # popularity: Zipf-like
        self.prod_weights = [1 / (i ** 0.8) for i in range(1, self.n_products + 1)]
        self.rng.shuffle(self.prod_weights)
        self.price_now = {p["product_id"]: p["unit_price"] for p in self.products}

    # ------------------------------------------------------------------- orders
    def gen_orders(self):
        self.orders = []          # final state of each order (for payments / clickstream)
        order_id, item_id = 100000, 1
        day = START.date()
        while day <= END.date():
            n = int(self.rng.gauss(self.base_daily_orders * seasonal_weight(day), 6))
            for _ in range(max(n, 0)):
                placed = self.rand_ts(day)
                # repeat customers: choose among customers already signed up
                cid = self.rng.randint(1, self.n_customers)
                tries = 0
                while self.signup_by_id[cid] > placed and tries < 10:
                    cid = self.rng.randint(1, self.n_customers); tries += 1
                if self.signup_by_id[cid] > placed:
                    continue
                order_id += 1
                cust = self.customers[cid - 1]
                items, subtotal, discount_total = [], 0.0, 0.0
                for pid in set(self.rng.choices(range(1, self.n_products + 1), self.prod_weights, k=self.rng.choices([1, 2, 3, 4], [55, 28, 12, 5])[0])):
                    qty = self.rng.choices([1, 2, 3], [80, 15, 5])[0]
                    price = self.price_now[pid]
                    disc = round(price * qty * self.rng.choice([0, 0, 0, 0.05, 0.1, 0.15]), 2)
                    items.append({"order_item_id": item_id, "order_id": order_id, "product_id": pid,
                                  "quantity": qty, "unit_price": price, "discount_amount": disc,
                                  "updated_at": ts(placed)})
                    item_id += 1
                    subtotal += price * qty
                    discount_total += disc
                shipping = 0 if subtotal >= 499 else 49
                total = round(subtotal - discount_total + shipping, 2)
                promo = self.rng.choice([None, None, None, "FEST10", "NEWUSER", "FLAT200"]) if discount_total else None
                base = {"order_id": order_id, "customer_id": cid, "order_ts": ts(placed), "order_status": "placed",
                        "channel": self.rng.choice(CHANNELS), "shipping_city": cust["city"], "promo_code": promo,
                        "shipping_fee": shipping, "updated_at": ts(placed)}
                self.add_cdc("orders", "I", placed, base)
                for it in items:
                    self.add_cdc("order_items", "I", placed, it)

                # lifecycle
                r = self.rng.random()
                if r < 0.06:
                    path = [("cancelled", self.rng.randint(1, 24))]
                elif r < 0.11:
                    path = [("shipped", self.rng.randint(12, 48)), ("delivered", self.rng.randint(48, 120)), ("returned", self.rng.randint(120, 400))]
                else:
                    path = [("shipped", self.rng.randint(12, 48)), ("delivered", self.rng.randint(48, 120))]
                status, last = "placed", placed
                for st, hrs in path:
                    at = placed + timedelta(hours=hrs, minutes=self.rng.randint(0, 59))
                    if at > END:
                        break
                    status, last = st, at
                    self.add_cdc("orders", "U", at, {**base, "order_status": st, "updated_at": ts(at)})
                self.orders.append({"order_id": order_id, "customer_id": cid, "placed": placed, "status": status,
                                    "status_at": last, "total": total, "items": items, "channel": base["channel"]})
            day += timedelta(days=1)

        # hard deletes: GDPR test orders removed from OLTP (~0.1%)
        for o in self.rng.sample(self.orders, k=max(1, len(self.orders) // 1000)):
            at = o["status_at"] + timedelta(days=1)  # delete is the last change for the order
            if at < END:
                self.add_cdc("orders", "D", at, {"order_id": o["order_id"], "customer_id": o["customer_id"],
                                                  "order_ts": ts(o["placed"]), "order_status": o["status"],
                                                  "channel": o["channel"], "shipping_city": None, "promo_code": None,
                                                  "shipping_fee": None, "updated_at": ts(at)})
                o["deleted"] = True
                self.stats["orders_hard_deleted"] += 1

        # DMS re-delivers some records after a task restart -> exact duplicates
        dups = self.rng.sample(self.cdc["orders"], k=len(self.cdc["orders"]) // 200)
        self.cdc["orders"].extend(dict(d) for d in dups)
        self.stats["orders_duplicate_cdc_rows"] = len(dups)

    # ------------------------------------------------------------------ payments
    def gen_payments(self):
        self.payments = []
        for o in self.orders:
            if o.get("deleted"):
                continue
            method = self.rng.choice(PAY_METHODS)
            # failed first attempt for ~5% of prepaid orders
            if method != "COD" and self.rng.random() < 0.05:
                self.payments.append(self._payment(o, method, "failed", o["placed"] + timedelta(seconds=40)))
            if self.rng.random() < 0.002:
                self.stats["orders_missing_payment"] += 1
                continue  # DQ issue: payment never synced from gateway
            if method == "COD":
                if o["status"] in ("delivered", "returned"):
                    p = self._payment(o, method, "captured", o["placed"] + timedelta(hours=72))
                else:
                    continue  # COD not collected yet / cancelled
            else:
                p = self._payment(o, method, "captured", o["placed"] + timedelta(seconds=90))
            if self.rng.random() < 0.003:
                p["amount"] = round(p["amount"] + self.rng.choice([-1, 1]) * self.rng.uniform(1, 150), 2)
                self.stats["payments_amount_mismatch"] += 1
            self.payments.append(p)
            if o["status"] in ("cancelled", "returned") and method != "COD":
                self.payments.append(self._payment(o, method, "refunded", o["status_at"] + timedelta(hours=26),
                                                   parent=p["payment_id"]))

    def _payment(self, o, method, status, at, parent=None):
        pay = {
            "payment_id": f"pay_{uuid.UUID(int=self.rng.getrandbits(128)).hex[:20]}",
            "order_id": o["order_id"], "amount": o["total"], "currency": "INR",
            "method": method, "status": status, "gateway_ts": at.strftime("%Y-%m-%dT%H:%M:%S+05:30"),
            "parent_payment_id": parent,
            "fees": {"gateway_fee": round(o["total"] * (0.0 if method == "COD" else 0.018), 2),
                     "gst": round(o["total"] * (0.0 if method == "COD" else 0.018) * 0.18, 2)},
        }
        if method == "card":
            pay["card"] = {"network": self.rng.choice(["VISA", "MASTERCARD", "RUPAY"]), "last4": f"{self.rng.randint(0, 9999):04d}"}
        if method == "UPI":
            pay["upi"] = {"app": self.rng.choice(["GPay", "PhonePe", "Paytm", "BHIM"])}
        return pay

    # --------------------------------------------------------------- clickstream
    def gen_clickstream(self):
        events_by_day = defaultdict(list)

        def session(customer_id, start, converts, order=None):
            sid = uuid.UUID(int=self.rng.getrandbits(128)).hex
            device = self.rng.choices([("mobile", "Android"), ("mobile", "iOS"), ("desktop", "Windows"), ("desktop", "macOS"), ("tablet", "Android")], [45, 20, 22, 8, 5])[0]
            src, med, camp = self.rng.choice(UTM)
            t = start
            steps = ["page_view"]
            prods = [it["product_id"] for it in order["items"]] if order else self.rng.choices(range(1, self.n_products + 1), self.prod_weights, k=self.rng.randint(1, 3))
            for pid in prods:
                steps += [("product_view", pid)]
            if converts:
                steps += [("add_to_cart", p) for p in prods] + ["checkout_start", "purchase"]
            else:
                r = self.rng.random()
                if r < 0.30:
                    steps += [("add_to_cart", prods[0])]
                if r < 0.12:
                    steps += ["checkout_start"]
            out = []
            for st in steps:
                ev, pid = (st, None) if isinstance(st, str) else st
                t += timedelta(seconds=self.rng.randint(5, 140))
                e = {
                    "event_id": uuid.UUID(int=self.rng.getrandbits(128)).hex,
                    "event_type": ev, "event_ts": t.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                    "session_id": sid, "customer_id": customer_id,
                    "device": {"type": device[0], "os": device[1]},
                    "page": {"url": f"/p/{pid}" if pid else ("/checkout" if ev in ("checkout_start", "purchase") else "/"),
                             "referrer": None if src == "direct" else f"https://{src}.com"},
                    "utm": {"source": src, "medium": med, "campaign": camp},
                    "properties": {},
                }
                if pid:
                    e["properties"] = {"product_id": pid, "price": self.price_now[pid]}
                if ev == "purchase" and order:
                    e["properties"] = {"order_id": order["order_id"], "revenue": order["total"],
                                       "items": [{"product_id": i["product_id"], "qty": i["quantity"]} for i in order["items"]]}
                out.append(e)
            return out

        for o in self.orders:
            if o["placed"] < CLICKSTREAM_START or o["channel"] == "marketplace":
                continue
            evs = session(o["customer_id"], o["placed"] - timedelta(minutes=self.rng.randint(3, 25)), True, o)
            for e in evs:
                events_by_day[e["event_ts"][:10]].append(e)
        # non-converting sessions (some anonymous)
        day = CLICKSTREAM_START.date()
        while day <= END.date():
            for _ in range(int(200 * seasonal_weight(day))):
                cid = self.rng.randint(1, self.n_customers) if self.rng.random() < 0.55 else None
                for e in session(cid, self.rand_ts(day), False):
                    events_by_day[e["event_ts"][:10]].append(e)
            day += timedelta(days=1)

        # late arrivals: ~0.5% of events land in the NEXT day's file (mobile offline buffering)
        days = sorted(events_by_day)
        for d, nxt in zip(days, days[1:]):
            late = [e for e in events_by_day[d] if self.rng.random() < 0.005]
            if late:
                ids = {e["event_id"] for e in late}
                events_by_day[d] = [e for e in events_by_day[d] if e["event_id"] not in ids]
                events_by_day[nxt].extend(late)
                self.stats["clickstream_late_events"] += len(late)

        root = self.out / "clickstream"
        for d, evs in sorted(events_by_day.items()):
            evs.sort(key=lambda e: e["event_ts"])
            lines = [json.dumps(e) for e in evs]
            # DQ: duplicates (at-least-once delivery) + malformed JSON
            for _ in range(len(lines) // 250):
                lines.insert(self.rng.randint(0, len(lines)), self.rng.choice(lines)); self.stats["clickstream_dup_events"] += 1
            for _ in range(len(lines) // 800):
                bad = self.rng.choice(lines)[: self.rng.randint(20, 80)]
                lines.insert(self.rng.randint(0, len(lines)), bad); self.stats["clickstream_malformed"] += 1
            p = root / f"dt={d}"
            p.mkdir(parents=True, exist_ok=True)
            with gzip.open(p / f"events_{d.replace('-', '')}.json.gz", "wt", encoding="utf-8") as f:
                f.write("\n".join(lines) + "\n")
            self.stats["clickstream_events"] += len(evs)
            self.stats["clickstream_files"] += 1

    # ------------------------------------------------------------------- writers
    def write_cdc(self):
        cols = {
            "customers": ["Op", "cdc_ts", "customer_id", "first_name", "last_name", "email", "phone", "city", "state", "country", "segment", "signup_ts", "updated_at"],
            "products": ["Op", "cdc_ts", "product_id", "sku", "product_name", "category", "sub_category", "brand", "unit_price", "unit_cost", "is_active", "updated_at"],
            "orders": ["Op", "cdc_ts", "order_id", "customer_id", "order_ts", "order_status", "channel", "shipping_city", "promo_code", "shipping_fee", "updated_at"],
            "order_items": ["Op", "cdc_ts", "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount", "updated_at"],
        }
        for table, rows in self.cdc.items():
            by_month = defaultdict(list)
            for r in rows:
                by_month[r["cdc_ts"][:7]].append(r)
            for ym, rs in sorted(by_month.items()):
                y, m = ym.split("-")
                p = self.out / "dms" / table / f"year={y}" / f"month={m}"
                p.mkdir(parents=True, exist_ok=True)
                with open(p / f"{table}_{y}{m}.csv", "w", newline="", encoding="utf-8") as f:
                    w = csv.DictWriter(f, fieldnames=cols[table], extrasaction="ignore")
                    w.writeheader()
                    for r in sorted(rs, key=lambda x: x["cdc_ts"]):
                        w.writerow({k: ("" if v is None else v) for k, v in r.items()})
            self.stats[f"cdc_rows_{table}"] = len(rows)

    def write_payments_api(self, page_size=500):
        p = self.out / "_mock_payments_api"
        p.mkdir(parents=True, exist_ok=True)
        pays = sorted(self.payments, key=lambda x: x["gateway_ts"])
        pages = [pays[i:i + page_size] for i in range(0, len(pays), page_size)]
        for i, page in enumerate(pages, 1):
            body = {"data": page, "page": i, "page_size": page_size, "has_more": i < len(pages),
                    "next_cursor": f"page_{i + 1:04d}" if i < len(pages) else None}
            (p / f"page_{i:04d}.json").write_text(json.dumps(body), encoding="utf-8")
        self.stats["payments"] = len(pays)
        self.stats["payment_api_pages"] = len(pages)

    def run(self):
        if self.out.exists():
            shutil.rmtree(self.out)
        self.out.mkdir(parents=True)
        self.gen_customers(); self.gen_products(); self.gen_orders()
        self.gen_payments(); self.gen_clickstream()
        self.write_cdc(); self.write_payments_api()
        self.stats["orders"] = len(self.orders)
        (self.out / "_generation_manifest.json").write_text(json.dumps(dict(self.stats), indent=2))
        return dict(self.stats)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default="landing")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--customers", type=int, default=6000)
    ap.add_argument("--products", type=int, default=400)
    ap.add_argument("--daily-orders", type=float, default=32)
    a = ap.parse_args()
    stats = Generator(Path(a.out), a.seed, a.customers, a.products, a.daily_orders).run()
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
