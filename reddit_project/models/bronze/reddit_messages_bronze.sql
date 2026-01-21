{{ config(materialized='view') }}

SELECT *
FROM `reddit-484216.reddit_pipeline.reddit_messages`
