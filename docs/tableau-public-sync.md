# Tableau Public Sync

The primary dashboard is served through Streamlit. The Tableau Public version needs one extra bridge because Tableau Public cannot connect directly to the private PostgreSQL database used by this project.

## Approach

A scheduled Google Apps Script job syncs selected PostgreSQL tables into Google Sheets. Tableau Public then reads from those sheets.

The sync uses two modes:

- `append`: add only records newer than the latest date already present in the sheet.
- `upsert`: update or insert lookup records based on a primary key and freshness/check column.

## Tables Synced

| Table | Mode | Purpose |
| --- | --- | --- |
| `mart_price_news__analysis` | append | Main dashboard mart combining price, volume, and sentiment metrics |
| `biz_info_lookup` | upsert | Company profile and financial metadata by symbol |
| `mart_price_vol_chgn` | append | Daily price and volume movement |
| `stg_price` | append | Supporting price data for dashboard drill-downs |
| `mart_news__recent` | append | Recent ticker-related news |

## Operational Notes

- The script skips weekends.
- Database credentials are stored in Apps Script properties, not in the spreadsheet.
- A single JDBC connection is reused across table syncs.
- Partial failures are collected and emailed after the run.
- Append tables use the latest synced date to avoid reloading old rows.

## Simplified Sync Configuration

```javascript
const syncConfigs = [
  {
    tableName: "mart_price_news__analysis",
    syncMode: "append",
    dateColName: "date",
    dateColIndex: 1
  },
  {
    tableName: "biz_info_lookup",
    syncMode: "upsert",
    pkCol: "Symbol",
    checkCol: "LatestQuarter"
  },
  {
    tableName: "mart_price_vol_chgn",
    syncMode: "append",
    dateColName: "extraction_date",
    dateColIndex: 1
  },
  {
    tableName: "stg_price",
    syncMode: "append",
    dateColName: "extraction_date",
    dateColIndex: 1
  },
  {
    tableName: "mart_news__recent",
    syncMode: "append",
    dateColName: "extraction_date",
    dateColIndex: 1
  }
];
```
