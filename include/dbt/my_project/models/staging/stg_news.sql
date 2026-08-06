-- models/staging/stg_news.sql
{{ 
  config(
    materialized='incremental',
    unique_key=['extraction_date', 'url', 'mentioned_ticker']
) }}

WITH source_data AS (
  SELECT date, new1, new2, new3
  FROM {{ source('stocks_db', 'raw_most_active_stocks') }}
  {% if is_incremental() %}
  WHERE date > (SELECT max(extraction_date) FROM {{ this }})
  {% endif %}
),

unioned AS (
  SELECT date, new1 AS news_json FROM source_data WHERE new1 IS NOT NULL
  UNION ALL
  SELECT date, new2 FROM source_data WHERE new2 IS NOT NULL
  UNION ALL
  SELECT date, new3 FROM source_data WHERE new3 IS NOT NULL
),

parsed AS (
  SELECT
    date AS extraction_date,
    news_json::jsonb AS nj
  FROM unioned
  WHERE news_json IS NOT NULL
),

feed AS (
  SELECT
    p.extraction_date,
    f.elem ->> 'url'            AS url,
    f.elem ->> 'title'          AS title,
    f.elem ->> 'summary'        AS summary,
    TO_TIMESTAMP(f.elem ->> 'time_published', 'YYYYMMDD"T"HH24MISS')::date AS time_published_date,
    ts.elem ->> 'ticker'                            AS mentioned_ticker,
    (ts.elem ->> 'relevance_score')::float          AS ticker_relevance_score,
    ts.elem ->> 'ticker_sentiment_label'            AS ticker_sentiment_label,
    (ts.elem ->> 'ticker_sentiment_score')::float   AS ticker_sentiment_score
  FROM parsed p
  LEFT JOIN LATERAL jsonb_array_elements(p.nj -> 'feed') AS f(elem) ON true
  LEFT JOIN LATERAL jsonb_array_elements(f.elem -> 'ticker_sentiment') AS ts(elem) ON true
)

SELECT
  extraction_date,
  url,
  title,
  summary,
  time_published_date,
  mentioned_ticker,
  ticker_relevance_score,
  ticker_sentiment_label,
  ticker_sentiment_score
FROM feed