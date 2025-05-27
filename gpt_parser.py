import re

def parse_prompt(prompt: str) -> dict:
    prompt = prompt.lower()
    result = {
        "change_pct": None,
        "interval_minutes": None,
        "fundamentals": {},
        "indicators": {},
        "patterns": []
    }

    # % change
    pct_match = re.search(r"(\d+(\.\d+)?)\s*%", prompt)
    if pct_match:
        result["change_pct"] = float(pct_match.group(1))

    # Time interval
    time_match = re.search(r"(\d{1,3})\s*(minutes|min|m)", prompt)
    if time_match:
        result["interval_minutes"] = int(time_match.group(1))

    # Fundamentals
    fundamentals = {
        "p/e": "pe_ratio", "pe": "pe_ratio",
        "eps": "eps", "roe": "roe"
    }
    for text, key in fundamentals.items():
        match = re.search(rf"{text}\s*(<=|>=|<|>|=)\s*(\d+(\.\d+)?)", prompt)
        if match:
            result["fundamentals"][key] = match.group(1) + match.group(2)

    # Technical indicators
    indicators = ["rsi", "macd", "ema", "sma", "boll", "cci", "atr"]
    for ind in indicators:
        match = re.search(rf"{ind}[ _]?(\d+)?\s*(<=|>=|<|>|=)\s*(\d+(\.\d+)?)", prompt)
        if match:
            name = ind if not match.group(1) else f"{ind}{match.group(1)}"
            result["indicators"][name] = match.group(2) + match.group(3)

    # Chart patterns
    chart_keywords = ["bullish engulfing", "bearish engulfing", "doji", "hammer", "shooting star", "breakout", "support retest"]
    for pattern in chart_keywords:
        if pattern in prompt:
            result["patterns"].append(pattern)

    return result