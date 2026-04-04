
import pandas as pd
import re
import os
import numpy as np

# =========================
# SET WORKING DIRECTORY
# =========================
os.chdir(r"C:\Users\rinthoma\OneDrive - Publicis Groupe\Documents\TSP\DMA_2026")

SEGMENTS     = ["DMA_Amarillo", "DMA_Atlanta", "DMA_Austin",
                "DMA_Baltimore", "DMA_Chicago", "DMA_Cleveland", "DMA_ColumbusGA", "DMA_CorpusChristi",
                "DMA_Denver", "DMA_DFW", "DMA_FtMyers", "DMA_Houston", "DMA_Jacksonville",
                "DMA_Knoxville", "DMA_LasVegas", "DMA_LosAngeles", "DMA_Memphis", "DMA_Miami - Ft. Lauderdale",
                "DMA_MidSouth", "DMA_Mobile - Pensacola", "DMA_NewYork", "DMA_Orlando-Daytona-Melbourne", "DMA_Phoenix",
                "DMA_Reno", "DMA_Sacramento", "DMA_SanDiego", "DMA_SanFrancisco",
                "DMA_SeattleTacoma", "DMA_Spokane", "DMA_TampaStPete", "DMA_Tucson",
                "DMA_Waco", "DMA_WashingtonDC", "DMA_WestPalmBeach",
                "Nationwide DMA_ANMS", "Nationwide DMA_ANPCOM", "Nationwide DMA_ANUSA",
                ]

# =========================
# GET PROFILE FILES
# =========================
profile_files = [
    f for f in os.listdir(os.getcwd())
    if f.lower().endswith(".xlsx") and "profile" in f.lower()
]

# =========================
# BUILD (HEADER, CATEGORY)
# =========================
pairs = []
seen = set()

for file in profile_files:
    print(f"Processing: {file}")

    wb = pd.ExcelFile(file)

    for sheet in wb.sheet_names:
        if not any(x in sheet.lower() for x in [
            "demographic", "financial", "lifestyles",
            "automotive", "market"
        ]):
            continue

        df = pd.read_excel(file, sheet_name=sheet, header=None, usecols=[0])
        df.columns = ["text"]

        current_header = None

        for cell in df["text"]:
            if pd.isna(cell):
                continue

            val = str(cell).strip()
            if not val:
                continue

            # HEADER (ALL CAPS)
            if val == val.upper() and any(c.isalpha() for c in val):
                current_header = val
                continue

            # CATEGORY
            if current_header:
                tup = (current_header, val)
                if tup not in seen:
                    seen.add(tup)
                    pairs.append(tup)

df_pairs = pd.DataFrame(pairs, columns=["Header", "Category"])
print(df_pairs.head())

# =========================
# MULTIPLY BY SEGMENTS
# =========================
df_segments = pd.DataFrame(SEGMENTS, columns=["Segment"])

df_pairs2 = (
    df_pairs
    .merge(df_segments, how="cross")
    .reset_index(drop=True)
)

df_pairs2.to_excel("df_pairs2.xlsx", index=False)
print(df_pairs2.shape)

# =========================
# DATA FILE
# =========================
file_path = r"C:\Users\rinthoma\OneDrive - Publicis Groupe\Documents\TSP\DMA_2026\Data_Sheet2.xlsx"

# =========================
# DMA FILTER BY SHEET
# =========================
dmas_by_sheet = {
    "Data1": ["DMA_Amarillo", "DMA_Atlanta", "DMA_Austin", "DMA_Baltimore", "DMA_Chicago", "DMA_Cleveland", "DMA_ColumbusGA"],
    "Data1": ["DMA Corpus Christi", "DMA_Denver", "DMA_DFW", "DMA_FtMyers", "DMA_Houston", "DMA_Jacksonville",
              "DMA Knoxville", "DMA Las Vegas", "DMA Los Angeles", "DMA Memphis"],
    "Data3": ["DMA_Miami - Ft. Lauderdale", "DMA_MidSouth", "DMA_Mobile - Pensacola", "DMA_NewYork", "DMA_Orlando-Daytona-Melbourne", "DMA_Phoenix",
              "DMA_Reno", "DMA_Sacramento", "DMA_SanDiego", "DMA_SanFrancisco"],
    "Data4": ["DMA_SeattleTacoma", "DMA_Spokane", "DMA_TampaStPete", "DMA_Tucson",
              "DMA_Waco", "DMA_WashingtonDC", "DMA_WestPalmBeach"
              ],
    "Data5": ["Nationwide DMA_ANMS", "Nationwide DMA_ANPCOM", "Nationwide DMA_ANUSA"]
}

# =========================
# GET DATA SHEETS
# =========================
with pd.ExcelFile(file_path) as xls:
    sheets = [s for s in xls.sheet_names if s.lower().startswith("data")]

# =========================
# PROCESS EACH SHEET
# =========================
def process_sheet(sheet_name: str) -> pd.DataFrame:
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    # Find header row
    hdr_idx = df.index[
        df.iloc[:, 0].astype(str).str.contains("Characteristic", na=False)
    ]

    if len(hdr_idx) == 0:
        return pd.DataFrame(columns=["sheet", "key", "dma", "val1", "val2"])

    hdr = hdr_idx[0]

    # Map DMA -> column
    header_pos = {
        str(df.iat[hdr, c]).strip(): c
        for c in range(df.shape[1])
        if str(df.iat[hdr, c]).strip()
    }

    available_dmas = [
        s for s, c in header_pos.items()
        if (c + 2) < df.shape[1] and s != "nan"
    ]

    requested = dmas_by_sheet.get(sheet_name)
    dmas = available_dmas if not requested else [d for d in requested if d in available_dmas]

    if not dmas:
        return pd.DataFrame(columns=["sheet", "key", "dma", "val1", "val2"])

    keys = df_pairs2["Category"].dropna().astype(str).str.strip().drop_duplicates().tolist()

    if not keys:
        return pd.DataFrame(columns=["sheet", "key", "dma", "val1", "val2"])

    # Build grid
    inputs = (
        pd.DataFrame({"key": keys}).assign(_=1)
        .merge(pd.DataFrame({"dma": dmas, "_": 1}), on="_")
        .drop(columns="_")
    )

    def fetch_two(key_value, dma):
        base_col = header_pos.get(dma)
        if base_col is None:
            return (np.nan, np.nan)

        match = df.iloc[:, 0].astype(str).str.strip() == str(key_value).strip()
        if not match.any():
            return (np.nan, np.nan)

        r = match.idxmax()
        out = []

        for off in (1, 2):
            try:
                out.append(float(df.iat[r, base_col + off]) / 100.0)
            except:
                out.append(np.nan)

        return tuple(out)

    inputs[["val1", "val2"]] = [
        fetch_two(k, d) for k, d in zip(inputs["key"], inputs["dma"])
    ]

    inputs.insert(0, "sheet", sheet_name)
    return inputs

# =========================
# RUN ALL SHEETS
# =========================
all_results = pd.concat(
    [process_sheet(s) for s in sheets],
    ignore_index=True
)

print(all_results.head())

# =========================
# SAMPLE FACTORS
# =========================
def read_sample_factors(file_path, sheet_name="Sample Factors"):
    df = pd.read_excel(file_path, sheet_name=sheet_name, header=None)

    hdr = df.index[
        df.iloc[:, 0].astype(str).str.contains("Segment Names", case=False, na=False)
    ]

    if len(hdr) == 0:
        return pd.DataFrame(columns=["dma", "client_sf", "tsp_sf"])

    hdr = hdr[0]

    header_pos = {
        str(df.iat[hdr, c]).strip(): c
        for c in range(df.shape[1])
        if str(df.iat[hdr, c]).strip() not in ("", "nan")
    }

    def find_row(label):
        col0 = df.iloc[:, 0].astype(str).str.lower().str.replace(":", "").str.strip()
        hit = df.index[col0 == label.lower()]
        return hit[0] if len(hit) else None

    r_client = find_row("Client Sample Factor")
    r_tsp = find_row("TSP Sample Factor")

    def to_float(x):
        if pd.isna(x):
            return np.nan
        if isinstance(x, str):
            x = x.replace(",", "")
        try:
            return float(x)
        except:
            return np.nan

    rows = []
    for seg, c in header_pos.items():
        rows.append({
            "dma": seg,
            "client_sf": to_float(df.iat[r_client, c]) if r_client else np.nan,
            "tsp_sf": to_float(df.iat[r_tsp, c]) if r_tsp else np.nan
        })

    return pd.DataFrame(rows)

# Merge sample factors
sf = read_sample_factors(file_path)
all_results = all_results.merge(sf, on="dma", how="left")

# =========================
# ADD HEADER MAPPING
# =========================
df_pairs3 = df_pairs2[["Category", "Header"]].drop_duplicates("Category")

all_results2 = all_results.merge(
    df_pairs3,
    left_on="key",
    right_on="Category",
    how="left"
)

# =========================
# FINAL OUTPUT
# =========================
all_results3 = (
    all_results2
    .sort_values(by=["dma", "Header"])
    .drop(columns=["Category"])
)

all_results3.to_excel("all_results3.xlsx", index=False)

print("✅ DONE")