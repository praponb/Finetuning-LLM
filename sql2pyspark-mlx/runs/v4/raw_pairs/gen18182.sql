WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        DATE_FORMAT(ends_on, 'yyyy-MM') AS ends_on_label
    FROM promotions
    WHERE starts_on >= '2025-08-30'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM base
    INTERSECT ALL
    SELECT id
    FROM promotions
    WHERE ends_on > '2024-05-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.id = k.id
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct
    FROM reconciled s
    JOIN promotions pr
        ON s.channel <=> pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on < pr.ends_on + INTERVAL 30 DAYS
),
base_promotions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_ADD(starts_on, 5) AS starts_on_plus_5d,
        ADD_MONTHS(ends_on, 6) AS ends_on_plus_months,
        COALESCE(channel, 'DIRECT') AS channel_clean
    FROM attributed
    WHERE ends_on > DATE '2023-06-25'
      AND channel IS NOT NULL
),
remaining_keys_2 AS (
    -- Set intersection on keys
    SELECT id, promo_code
    FROM base_promotions
    INTERSECT ALL
    SELECT id, promo_code
    FROM promotions
    WHERE discount_pct > 0.17
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base_promotions s
    JOIN remaining_keys_2 k ON s.id = k.id AND s.promo_code = k.promo_code
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN discount_pct >= 0.33 THEN 'PREMIUM' ELSE 'STANDARD' END AS discount_pct_tier,
    SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
    COUNT(DISTINCT channel) AS distinct_channels
FROM verified
GROUP BY discount_pct_tier
ORDER BY discount_pct_tier DESC NULLS LAST;
