{{ config(
    materialized='table',
    partition_by={
      "field": "report_date",
      "data_type": "date"
    }
) }}

WITH daily_aggregates AS (
  SELECT
    -- Izvuci datum i nazovi ga report_date
    DATE(created_ts) AS report_date,

    sentiment,
    COUNT(*) AS post_count,
    AVG(score_filled) AS avg_score,
    SUM(score_filled) AS total_score,
    COUNT(DISTINCT author) AS unique_authors
  FROM {{ ref('reddit_messages_silver') }}
  -- Grupiraj po report_date i sentimentu
  GROUP BY report_date, sentiment
),

window_calculations AS (
  SELECT
    *,
    -- Rolling sum zadnjih 7 dana (6 PRECEDING + CURRENT)
    SUM(post_count) OVER (
      PARTITION BY sentiment
      ORDER BY report_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ) AS rolling_7d_posts,

    -- Prethodni dan
    LAG(post_count) OVER (
      PARTITION BY sentiment
      ORDER BY report_date
    ) AS prev_day_posts
  FROM daily_aggregates
)

SELECT
  *,
  -- Day-over-Day % (izbjegni dijeljenje s nulom)
  CASE
    WHEN prev_day_posts > 0 THEN ((post_count - prev_day_posts) / prev_day_posts) * 100
    ELSE NULL
  END AS day_over_day_change_pct
FROM window_calculations
