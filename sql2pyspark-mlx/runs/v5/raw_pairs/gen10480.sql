WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        ADD_MONTHS(ends_on, 6) AS ends_on_plus_months,
        DAYOFWEEK(starts_on) AS starts_on_dayofweek
    FROM promotions
    WHERE channel IS NOT NULL
      AND starts_on < '2023-11-01'
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY ends_on_plus_months, id) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT channel
    FROM latest
    INTERSECT ALL
    SELECT channel
    FROM promotions
    WHERE discount_pct <= 0.19
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN matched_keys k ON s.channel = k.channel
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(starts_on_dayofweek) OVER w_recent AS promo_code_min_starts_on_dayofweek,
        MAX(starts_on_dayofweek) OVER w_grp AS channel_max_starts_on_dayofweek
    FROM verified
    WINDOW w_recent AS (PARTITION BY promo_code, channel), w_grp AS (PARTITION BY channel)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MIN(promo_code_min_starts_on_dayofweek) OVER w_part AS promo_code_min_promo_code_min_starts_on_dayofweek,
        COUNT(promo_code_min_starts_on_dayofweek) OVER w_recent AS promo_code_count_promo_code_min_starts_on_dayofweek,
        RANK() OVER (PARTITION BY promo_code ORDER BY starts_on_dayofweek NULLS LAST) AS starts_on_dayofweek_rank,
        MAX(starts_on_dayofweek) OVER w_part AS promo_code_max_starts_on_dayofweek,
        MAX(promo_code_min_starts_on_dayofweek) OVER w_part AS promo_code_max_promo_code_min_starts_on_dayofweek
    FROM enriched
    WINDOW w_part AS (PARTITION BY promo_code), w_recent AS (PARTITION BY promo_code, channel)
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', ends_on) AS quarter_start,
    channel,
    APPROX_PERCENTILE(promo_code_min_promo_code_min_starts_on_dayofweek, ARRAY(0.25, 0.5, 0.75)) AS promo_code_min_promo_code_min_starts_on_dayofweek_quartiles,
    ROUND(SUM(promo_code_min_starts_on_dayofweek) / NULLIF(COUNT(CASE WHEN channel = 'mobile' THEN 1 END), 0), 2) AS promo_code_min_starts_on_dayofweek_per_mobile,
    ROUND(AVG(promo_code_min_promo_code_min_starts_on_dayofweek), 2) AS avg_promo_code_min_promo_code_min_starts_on_dayofweek,
    COUNT_IF(channel = 'partner') AS partner_events
FROM with_windows
GROUP BY DATE_TRUNC('QUARTER', ends_on), channel
HAVING ROUND(AVG(promo_code_min_promo_code_min_starts_on_dayofweek), 2) > 0
