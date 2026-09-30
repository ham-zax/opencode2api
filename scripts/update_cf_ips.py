#!/usr/bin/env python3
"""
Cloudflare IP Latency Tester & Daily 500ip Generator for OpenCode2API

Tests TLS handshake round-trip latency to opencode.ai across Cloudflare Anycast IPs,
ranks them by speed, and generates formatted 500ip.txt and cfip_opencode_formatted.txt.
"""
import os, sys, time, socket, ssl, re, datetime, concurrent.futures

TARGET_HOST = "opencode.ai"
PORT = 443
TIMEOUT = 2.0
MAX_WORKERS = 64
TOP_COUNT = 500

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SEED_FILES = [
    os.path.join(ROOT_DIR, "500ip.txt"),
    os.path.join(ROOT_DIR, "cfip_opencode_formatted.txt"),
    os.path.join(ROOT_DIR, "cfip_opencode_ok.txt")
]
OUT_500IP = os.path.join(ROOT_DIR, "500ip.txt")
OUT_FORMATTED = os.path.join(ROOT_DIR, "cfip_opencode_formatted.txt")

COUNTRY_NAMES = {
    "ca": ("ca", "Canada", "CA"),
    "us": ("us", "United States", "US"),
    "gb": ("gb", "United Kingdom", "GB"),
    "de": ("de", "Germany", "DE"),
    "fr": ("fr", "France", "FR"),
    "jp": ("jp", "Japan", "JP"),
    "sg": ("sg", "Singapore", "SG"),
    "kr": ("kr", "South Korea", "KR"),
    "hk": ("hk", "Hong Kong", "HK"),
    "tw": ("tw", "Taiwan", "TW"),
    "nl": ("nl", "Netherlands", "NL"),
}

def load_seed_ips():
    ip_info = {}
    for fpath in SEED_FILES:
        if not os.path.exists(fpath):
            continue
        with open(fpath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # Extract IP and optional existing tags
                m = re.match(r"^([\d\.]+):?(\d+)?(?:#([a-z]+)\s+\[([^\]]+)\]\s+([A-Z]+))?", line)
                if m:
                    ip = m.group(1)
                    port = int(m.group(2) or 443)
                    cc = (m.group(3) or "ca").lower()
                    cname = m.group(4) or COUNTRY_NAMES.get(cc, ("ca", "Canada", "CA"))[1]
                    cc_up = m.group(5) or cc.upper()
                    if ip not in ip_info:
                        ip_info[ip] = {"port": port, "cc": cc, "country": cname, "cc_up": cc_up}
    return ip_info

def probe_cf_ip(item):
    ip, meta = item
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE

    t0 = time.perf_counter()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(TIMEOUT)
        s.connect((ip, meta["port"]))
        ss = ctx.wrap_socket(s, server_hostname=TARGET_HOST)
        latency = (time.perf_counter() - t0) * 1000
        ss.close()
        return {
            "ip": ip,
            "port": meta["port"],
            "cc": meta["cc"],
            "country": meta["country"],
            "cc_up": meta["cc_up"],
            "latency": round(latency, 2),
            "ok": True
        }
    except Exception:
        return {"ip": ip, "ok": False, "latency": 99999}

def main():
    print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Loading Cloudflare seed IPs...")
    seed_data = load_seed_ips()
    total_seeds = len(seed_data)
    print(f"Total candidate IPs to probe: {total_seeds}")

    print(f"Probing TLS handshake latency against {TARGET_HOST}:443 ({MAX_WORKERS} threads)...")
    results = []
    t_start = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        for res in executor.map(probe_cf_ip, seed_data.items()):
            if res["ok"]:
                results.append(res)
                if len(results) % 100 == 0:
                    print(f"  Tested... found {len(results)} responsive nodes so far")

    duration = round(time.time() - t_start, 2)
    print(f"Probing complete in {duration}s. Responsive nodes: {len(results)} / {total_seeds}")

    if not results:
        print("Error: No responsive Cloudflare IPs found.")
        sys.exit(1)

    # Sort ascending by latency
    results.sort(key=lambda x: x["latency"])
    top_500 = results[:TOP_COUNT]

    today_str = datetime.date.today().strftime("%Y-%m-%d")

    # Generate 500ip.txt
    header_500 = (
        f"# CF Daily Top {len(top_500)} Lowest Latency IPs (with Region Tags)\n"
        f"# Format: ip:443#country_code_lower [Country Name] COUNTRY_CODE_UPPER Latency={{latency}}ms\n"
        f"# Data Sources: 104.16.0.0/12, 172.64.0.0/13, 131.0.72.0/22\n"
        f"# Generated Time: {today_str}\n"
        f"# Total Samples: {len(results)} available, top {len(top_500)} lowest latency\n\n"
    )
    lines_500 = [
        f"{r['ip']}:{r['port']}#{r['cc']} [{r['country']}] {r['cc_up']} Latency={r['latency']}ms\n"
        for r in top_500
    ]
    with open(OUT_500IP, "w", encoding="utf-8") as f:
        f.write(header_500 + "".join(lines_500))

    # Generate cfip_opencode_formatted.txt
    header_formatted = (
        f"# GLM CF Optimized IPs (with Region Tags, fixed pull for worker 33.spf.xx.kg dashboard)\n"
        f"# Format: ip:443#country_code_lower [Country Name] COUNTRY_CODE_UPPER\n"
        f"# Data Sources: 104.16.0.0/12, 172.64.0.0/13, 131.0.72.0/22\n"
        f"# Generated Time: {today_str}\n"
        f"# Total Samples: {len(results)} available, top {len(top_500)} lowest latency\n\n"
    )
    lines_formatted = [
        f"{r['ip']}:{r['port']}#{r['cc']} [{r['country']}] {r['cc_up']}\n"
        for r in top_500
    ]
    with open(OUT_FORMATTED, "w", encoding="utf-8") as f:
        f.write(header_formatted + "".join(lines_formatted))

    print(f"\nSuccessfully generated:")
    print(f"  -> {OUT_500IP} ({len(top_500)} IPs, lowest latency: {top_500[0]['latency']}ms)")
    print(f"  -> {OUT_FORMATTED}")

if __name__ == "__main__":
    main()
