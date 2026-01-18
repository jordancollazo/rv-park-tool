# Ingest LoopNet Leads

## Goal
Scrape Florida Mobile Home Park and RV Park listings from LoopNet using Apify, normalize the data, and ingest it into the CRM database.

## Inputs
- **Apify API Token**: Must be set in `.env` as `APIFY_API_TOKEN`.
- **State**: Default `FL`.
- **Property Types**: Default `Mobile Home Park`, `RV Park`.
- **Search Query**: Constructed from state and types (e.g. "Florida Mobile Home Parks For Sale").

## Tool
`execution/scrape_loopnet_apify.py`

## Output
- **Database Updates**: New or updated rows in `leads` table.
- **Console Output**: Summary of scraped, inserted, and updated leads.

## Procedure
1.  **Check Environment**: Ensure `APIFY_API_TOKEN` is present.
2.  **Run Scraper**:
    ```powershell
    python execution/scrape_loopnet_apify.py --state FL --limit 100
    ```
3.  **Monitor**: Watch for Apify actor completion and credit usage.
4.  **Verify**: Check database for new leads with `source_query` containing "LoopNet".

## Edge Cases
- **No Results**: Scraper returns 0 items. Check search query in Apify console.
- **Rate Limits**: Apify run fails. Script should handle exceptions gracefully.
- ** normalization**: Address format might vary.
