import pandas as pd

df = pd.read_csv('output/leads_orlando_fl_20251221.csv')

print('\n=== ORLANDO LEAD GENERATION SUMMARY ===\n')
print(f'Total leads found: {len(df)}\n')

print('Score Distribution:')
print(df['site_score_1_10'].value_counts().sort_index())

print('\n=== TOP ACQUISITION TARGETS (Score <= 3) ===')
low_score = df[df['site_score_1_10'] <= 3].sort_values('site_score_1_10')
print(f'\nFound {len(low_score)} high-opportunity targets:\n')

for idx, row in low_score.iterrows():
    print(f"{row['name']}")
    print(f"  Score: {row['site_score_1_10']} | Phone: {row['phone']} | Rating: {row['google_rating']}")
    print(f"  Reason: {row['score_reasons']}")
    print()

print('\n=== MEDIUM OPPORTUNITY (Score 4-6) ===')
medium = df[(df['site_score_1_10'] >= 4) & (df['site_score_1_10'] <= 6)].sort_values('site_score_1_10')
print(f'Found {len(medium)} medium-opportunity targets')

print('\n=== WELL-MANAGED (Score 7+) ===')
high = df[df['site_score_1_10'] >= 7]
print(f'Found {len(high)} well-managed properties (competitive market)')
