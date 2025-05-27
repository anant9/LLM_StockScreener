import pandas as pd

def filter_fundamentals(filters: dict, universe: list) -> list:
    df = pd.read_csv("data/screener_fundamentals.csv")
    df = df[df["Symbol"].isin(universe)]

    for key, rule in filters.items():
        if key not in df.columns:
            continue
        if rule.startswith("<"):
            df = df[df[key] < float(rule[1:])]
        elif rule.startswith(">"):
            df = df[df[key] > float(rule[1:])]
        elif rule.startswith("="):
            df = df[df[key] == float(rule[1:])]

    return df["Symbol"].tolist()