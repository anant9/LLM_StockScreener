import pandas as pd

def load_nifty50_symbols():
    try:
        df = pd.read_excel("data/nifty50_all_symbols.xlsx")
        return df["Symbol"].dropna().unique().tolist()
    except Exception as e:
        print("Error loading stock list:", e)
        return []