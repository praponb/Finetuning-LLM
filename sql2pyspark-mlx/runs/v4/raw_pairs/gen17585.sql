WITH eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.order_id IN (7, 29, 30)
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        channel,
        shipping_fee,
        DATEDIFF(CURRENT_DATE(), order_date) AS days_since_order_date,
        ROUND(shipping_fee * 1.07, 2) AS shipping_fee_scaled,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM eligible
    WHERE order_date > DATE '2023-08-07'
      AND customer_id IS NOT NULL
      AND shipping_fee IS NULL
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(shipping_fee) OVER (PARTITION BY channel ORDER BY days_since_order_date, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS shipping_fee_avg_5_prior,
        AVG(shipping_fee) OVER (PARTITION BY channel ORDER BY days_since_order_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS shipping_fee_avg_6
    FROM valid
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee DESC, id) AS latest_rank
        FROM metrics
    ) t
    WHERE latest_rank = 1
),
remaining_keys AS (
    -- Set difference on keys
    SELECT store_id, channel
    FROM latest
    EXCEPT
    SELECT store_id, channel
    FROM orders
    WHERE customer_id IS NOT NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    JOIN remaining_keys k ON s.store_id = k.store_id AND s.channel = k.channel
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        shipping_fee,
        shipping_fee_scaled,
        resolved_channel,
        shipping_fee_avg_5_prior,
        shipping_fee_avg_6,
        latest_rank,
        ROUND(days_since_order_date * 100, 2) AS adjusted_days_since_order_date
    FROM reconciled
    WHERE store_id IN (18, 28, 38)
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN shipping_fee_avg_6 >= 38 THEN 'LARGE' ELSE 'SMALL' END AS shipping_fee_avg_6_tier,
        CASE WHEN adjusted_days_since_order_date >= 84 THEN 'HIGH' ELSE 'LOW' END AS adjusted_days_since_order_date_tier,
        PERCENTILE_APPROX(adjusted_days_since_order_date, ARRAY(0.25, 0.5, 0.75)) AS adjusted_days_since_order_date_quartiles,
        APPROX_PERCENTILE(shipping_fee_avg_6, 0.95) AS shipping_fee_avg_6_p95,
        ROUND(AVG(shipping_fee_scaled), 2) AS avg_shipping_fee_scaled,
        MAX(adjusted_days_since_order_date) AS max_adjusted_days_since_order_date
    FROM base
    GROUP BY GROUPING SETS ((CASE WHEN shipping_fee_avg_6 >= 38 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN adjusted_days_since_order_date >= 84 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN shipping_fee_avg_6 >= 38 THEN 'LARGE' ELSE 'SMALL' END))
)
SELECT
    shipping_fee_avg_6_tier,
    adjusted_days_since_order_date_tier,
    shipping_fee_avg_6_p95,
    avg_shipping_fee_scaled
FROM stats
ORDER BY shipping_fee_avg_6_tier DESC;
