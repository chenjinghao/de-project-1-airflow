-- models/staging/stg_prices.sql
{{ 
  config(
    materialized='incremental',
    unique_key=['extraction_date', 'symbol', 'price_date']
) }}

WITH source_data AS (
  SELECT date, price1, price2, price3
  FROM {{ source('stocks_db', 'raw_most_active_stocks') }}
  {% if is_incremental() %}
  WHERE date > (SELECT max(extraction_date) FROM {{ this }})
  {% endif %}
),

unioned AS (
  SELECT date, price1 AS price_json FROM source_data WHERE price1 IS NOT NULL
  UNION ALL
  SELECT date, price2 FROM source_data WHERE price2 IS NOT NULL
  UNION ALL
  SELECT date, price3 FROM source_data WHERE price3 IS NOT NULL
),

parsed AS (
  SELECT
    date AS extraction_date,
    price_json::jsonb AS pj
  FROM unioned
  WHERE price_json IS NOT NULL
),

expanded AS (
  SELECT
    p.extraction_date,
    p.pj -> 'Meta Data' ->> '2. Symbol' AS symbol,
    ts.key::date AS price_date,
    ts.value ->> '1. open'  AS open_price,
    ts.value ->> '2. high'  AS high_price,
    ts.value ->> '3. low'   AS low_price,
    ts.value ->> '4. close' AS close_price,
    ts.value ->> '5. volume' AS volume
  FROM parsed p
  CROSS JOIN LATERAL jsonb_each(p.pj -> 'Time Series (Daily)') AS ts(key, value)
)

SELECT
  extraction_date,
  symbol,
  price_date,
  open_price::numeric  AS open_price,
  high_price::numeric  AS high_price,
  low_price::numeric   AS low_price,
  close_price::numeric AS close_price,
  volume::bigint       AS volume
FROM expanded
ORDER BY price_date DESC