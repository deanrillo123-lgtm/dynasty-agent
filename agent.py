bf = stat.get("battersFaced")
so = stat.get("strikeOuts")
bb = stat.get("baseOnBalls")
ip = stat.get("inningsPitched")

def _ip_to_float(ip_val):
    # MLB Stats API often gives IP as a string like "12.1" (12 + 1/3)
    if ip_val is None:
        return None
    try:
        s = str(ip_val).strip()
        if "." not in s:
            return float(s)
        whole, frac = s.split(".", 1)
        whole_f = float(whole)
        if frac == "0":
            return whole_f
        if frac == "1":
            return whole_f + (1.0 / 3.0)
        if frac == "2":
            return whole_f + (2.0 / 3.0)
        # fallback: treat as decimal (rare)
        return float(s)
    except Exception:
        return None

ip_f = _ip_to_float(ip)

k_pct = _pct(so, bf) if bf not in (None, 0, "0") else None
bb_pct = _pct(bb, bf) if bf not in (None, 0, "0") else None

k9 = None
bb9 = None
if (k_pct is None or bb_pct is None) and ip_f not in (None, 0.0):
    try:
        if so is not None:
            k9 = round((float(so) * 9.0) / ip_f, 2)
        if bb is not None:
            bb9 = round((float(bb) * 9.0) / ip_f, 2)
    except Exception:
        pass

rows.append(
    {
        "Name": player,
        "Level": lvl,
        "GS": stat.get("gamesStarted"),
        "IP": ip,
        "ERA": stat.get("era"),
        "FIP": None,
        "K%": k_pct,
        "BB%": bb_pct,
        "K/9": k9,
        "BB/9": bb9,
    }
)
