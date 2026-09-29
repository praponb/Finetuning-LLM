WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        YEAR(ends_on) AS ends_on_year,
        LENGTH(promo_code) AS promo_code_length,
        COALESCE(channel, 'UNKNOWN') AS channel_clean
    FROM promotions
    WHERE starts_on > TO_DATE('2023-08-23')
      AND ends_on BETWEEN '2023-07-01' AND '2023-11-01'
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY channel ORDER BY starts_on DESC) AS rank_in_group
        FROM cleaned
    ) t
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(ends_on_year, TRUE) OVER (PARTITION BY promo_code ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_ends_on_year,
        FIRST_VALUE(promo_code_length, TRUE) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_promo_code_length,
        SUM(ends_on_year) OVER (PARTITION BY channel ORDER BY ends_on_year, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_ends_on_year
    FROM deduped
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(discount_pct, 1) OVER (ORDER BY first_known_promo_code_length, id) AS next_discount_pct,
        NTILE(4) OVER (PARTITION BY channel, promo_code ORDER BY first_known_ends_on_year, id) AS first_known_ends_on_year_bucket,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code, channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM with_windows
),
with_windows_promotions AS (
    -- Ranking and gap features
    SELECT
        id,
        channel,
        starts_on,
        ends_on_year,
        channel_clean,
        first_known_ends_on_year,
        first_known_promo_code_length,
        RANK() OVER (PARTITION BY channel ORDER BY ends_on_year DESC NULLS LAST) AS ends_on_year_rank,
        ROUND(first_known_ends_on_year_bucket / SUM(first_known_ends_on_year_bucket) OVER (PARTITION BY channel), 4) AS first_known_ends_on_year_bucket_share
    FROM enriched
    WHERE next_discount_pct >= 0.2
)
-- Multi-level aggregation
SELECT
    CASE WHEN first_known_ends_on_year >= 73 THEN 'HIGH' ELSE 'LOW' END AS first_known_ends_on_year_tier,
    COUNT_IF(channel = 'partner') AS partner_events,
    COUNT_IF(channel = 'web') AS web_events
FROM with_windows_promotions
GROUP BY CASE WHEN first_known_ends_on_year >= 73 THEN 'HIGH' ELSE 'LOW' END
HAVING partner_events > 1
