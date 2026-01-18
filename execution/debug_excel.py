import pandas as pd
from pathlib import Path

path = Path("data/fl_dor/millage_taxes_levied.xlsx")
xls = pd.ExcelFile(path)

# Let's look at the actual header row
sheet = 'Millage Rates'
# Read with header=3 (assuming row 3 is the header)
df = pd.read_excel(xls, sheet_name=sheet, header=3)
print(f"\n--- {sheet} Columns from header=3 ---")
print("Columns:", list(df.columns)[:15])
print("\nFirst 3 rows:")
print(df.head(3).to_string())
