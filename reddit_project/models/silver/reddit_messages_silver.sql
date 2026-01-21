{{ config(
    materialized='table',
    partition_by={
      "field": "created_ts",
      "data_type": "timestamp",
      "granularity": "day"
    },
    cluster_by=["sentiment", "subreddit"]
) }}

WITH base AS (
  SELECT
    id,
    title,
    author,
    subreddit,

    -- NULL score -> 0
    COALESCE(score, 0) AS score_filled,

    -- epoch sekunde -> TIMESTAMP
    TIMESTAMP_SECONDS(created_utc) AS created_ts,

    created_at
  FROM {{ ref('reddit_messages_bronze') }}
),

deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY id
      ORDER BY created_ts DESC
    ) AS rn
  FROM base
)

SELECT
  id,

  -- Čišćenje title-a:
  -- 1) ukloni specijalne znakove
  -- 2) trim
  -- 3) višestruki razmaci -> jedan
  REGEXP_REPLACE(
    TRIM(REGEXP_REPLACE(title, r'[^\\p{L}\\p{N}\\s]', ' ')),
    r'\\s+',
    ' '
  ) AS title_clean,

  author,
  subreddit,
  score_filled,
  created_ts,
  created_at,

  -- Sentiment logika
  CASE
    WHEN score_filled >= 50 THEN 'HIGH_ENGAGEMENT'
    WHEN score_filled >= 20 THEN 'POSITIVE'
    WHEN score_filled < 0 THEN 'NEGATIVE'
    ELSE 'NEUTRAL'
  END AS sentiment

FROM deduplicated
WHERE rn = 1
  AND title IS NOT NULL
  AND LENGTH(TRIM(title)) > 0
