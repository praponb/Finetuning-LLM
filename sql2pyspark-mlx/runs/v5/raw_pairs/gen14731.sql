WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        pos + 1 AS item_no,
        elem.product_id,
        elem.price,
        elem.promo
    FROM carts
    LATERAL VIEW POSEXPLODE(items) t AS pos, elem
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        item_no,
        value
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) x AS value
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        value,
        STDDEV_SAMP(item_no) OVER w_recent AS value_stddev_samp_item_no,
        SUM(item_no) OVER w_recent AS value_sum_item_no
    FROM flattened
    WHERE item_no <= 3
    WINDOW w_recent AS (PARTITION BY value)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(value_stddev_samp_item_no * 1.15, 2) AS adjusted_value_stddev_samp_item_no
    FROM enriched
    WHERE value IN ('WELCOME5', 'VIP')
      AND value IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(value_stddev_samp_item_no * 100, 2) AS adjusted_value_stddev_samp_item_no_2
    FROM base
    WHERE value IS NOT NULL
)
-- Multi-level aggregation
SELECT
    value,
    CASE WHEN adjusted_value_stddev_samp_item_no_2 >= 78 THEN 'LARGE' ELSE 'SMALL' END AS adjusted_value_stddev_samp_item_no_2_tier,
    ROUND(SUM(value_sum_item_no) / NULLIF(COUNT(CASE WHEN value = 'WELCOME5' THEN 1 END), 0), 2) AS value_sum_item_no_per_welcome5,
    PERCENTILE_APPROX(value_sum_item_no, 0.5) AS value_sum_item_no_p50,
    SIZE(COLLECT_SET(value)) AS num_values,
    COUNT_IF(value = 'WELCOME5') AS welcome5_events,
    SUM(CASE WHEN value = 'BUNDLE' THEN adjusted_value_stddev_samp_item_no ELSE 0 END) AS bundle_adjusted_value_stddev_samp_item_no,
    APPROX_PERCENTILE(value_stddev_samp_item_no, 0.9) AS value_stddev_samp_item_no_p90
FROM valid
GROUP BY GROUPING SETS ((value, CASE WHEN adjusted_value_stddev_samp_item_no_2 >= 78 THEN 'LARGE' ELSE 'SMALL' END), (value))
HAVING PERCENTILE_APPROX(value_sum_item_no, 0.5) < 1000000
ORDER BY value_stddev_samp_item_no_p90 DESC;
