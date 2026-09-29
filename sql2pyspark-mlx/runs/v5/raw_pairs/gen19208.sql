WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN channel = 'store' THEN 1 ELSE 0 END AS is_store,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM promotions
    WHERE discount_pct BETWEEN 0.16 AND 0.31
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM base src
    LEFT JOIN promotions pr
        ON src.channel <=> pr.channel
       AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY promo_id DESC NULLS FIRST, id, promo_id) AS rn
        FROM matched
    ) ranked
    WHERE rn = 1
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(promotion_promo_code) AS promotion_promo_code_lower,
        LAST_DAY(starts_on) AS starts_on_month_end,
        COALESCE(promotion_promo_code, 'DIRECT') AS promotion_promo_code_filled
    FROM deduped
    WHERE channel IS NOT NULL
      AND channel IS NOT DISTINCT FROM 'store'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        channel_clean,
        promo_id,
        promotion_discount_pct,
        promotion_promo_code_lower,
        starts_on_month_end,
        LAG(promo_id) OVER (PARTITION BY promotion_promo_code ORDER BY is_store, id, promo_id) AS prev_promo_id,
        LEAD(promotion_discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY is_store, id, promo_id) AS next_promotion_discount_pct,
        LAG(promotion_discount_pct) OVER (PARTITION BY promotion_promo_code ORDER BY is_store, id, promo_id) AS prev_promotion_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY promotion_promo_code ORDER BY is_store, id, promo_id) AS seq_no
    FROM filtered
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, channel
    FROM with_windows
    EXCEPT
    SELECT id, channel
    FROM promotions
    WHERE channel IN ('mobile', 'store', 'partner')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.channel = k.channel
),
grouped AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('MONTH', starts_on) AS activity_month,
        DATE_TRUNC('QUARTER', starts_on_month_end) AS quarter_start,
        AVG(promo_id) FILTER (WHERE channel = 'store') AS store_avg_promo_id,
        COUNT(DISTINCT id) AS distinct_ids
    FROM kept_rows
    GROUP BY DATE_TRUNC('MONTH', starts_on), DATE_TRUNC('QUARTER', starts_on_month_end)
    HAVING COUNT(DISTINCT id) >= 1
)
SELECT
    activity_month,
    quarter_start,
    store_avg_promo_id
FROM grouped
ORDER BY store_avg_promo_id DESC NULLS LAST, quarter_start;
