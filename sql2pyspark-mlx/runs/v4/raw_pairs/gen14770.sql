WITH base AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        SUBSTRING(channel, 1, 2) AS channel_prefix
    FROM promotions
    WHERE starts_on > DATE '2023-06-15'
      AND channel IS NOT NULL
),
filtered AS (
    SELECT *, CONCAT(channel, ' - ', promo_code) AS channel_promo_code
    FROM base
    WHERE starts_on > DATE '2024-03-03'
      AND channel IS NOT NULL
      AND channel = 'web'
),
metrics AS (
    SELECT
        *,
        SUM(discount_pct) OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct,
        LAG(discount_pct, 2) OVER (PARTITION BY channel, promo_code ORDER BY ends_on, id) AS prev_discount_pct
    FROM filtered
),
base_promotions AS (
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        channel_prefix,
        channel_promo_code,
        prev_discount_pct,
        CAST(cumulative_discount_pct AS DOUBLE) AS cumulative_discount_pct_num
    FROM metrics
    WHERE ends_on <= '2025-05-29'
),
stats AS (
    SELECT
        DATE_TRUNC('YEAR', starts_on) AS year_start,
        COUNT(*) AS cnt,
        SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
        SUM(CASE WHEN channel = 'store' THEN discount_pct ELSE 0 END) AS store_discount_pct,
        SORT_ARRAY(COLLECT_SET(channel)) AS channel_list_2,
        SUM(discount_pct) FILTER (WHERE channel = 'store') AS store_total_discount_pct
    FROM base_promotions
    GROUP BY DATE_TRUNC('YEAR', starts_on)
    HAVING COUNT(*) < 1
)
SELECT
    year_start,
    channel_list,
    channel_list_2,
    store_total_discount_pct
FROM stats;
