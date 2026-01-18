"""Check the Excel file structure."""
import pandas as pd

df = pd.read_excel('data/Export Results.xlsx', header=1)
print(f'Rows: {len(df)}')
print('Columns:')
for c in df.columns.tolist():
    print(f'  - {c}')
print()
print('First row sample:')
print(df.iloc[0])
