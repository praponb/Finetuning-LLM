WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(channel, ', ', status) AS channel_status,
        LENGTH(channel) AS channel_length
    FROM orders
    WHERE order_date >= DATE '2023-04-20'
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM valid
    EXCEPT
    SELECT id
    FROM orders
    WHERE store_id IN (29, 31)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.id = k.id
),
base AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        channel_status,
        channel_length,
        LENGTH(channel) AS channel_length_2,
        DATE_FORMAT(order_date, 'yyyy-MM') AS order_date_label
    FROM verified
    WHERE order_date >= DATE '2023-06-12'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        shipping_fee,
        channel_status,
        channel_length,
        channel_length_2,
        DATE_TRUNC('WEEK', order_date) AS order_date_period,
        COALESCE(channel, 'N/A') AS resolved_channel
    FROM base
    WHERE order_date >= DATE '2023-07-09'
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY store_id ORDER BY channel_length_2 DESC NULLS LAST) AS channel_length_2_percent_rank,
        COUNT(channel_length) OVER (PARTITION BY store_id ORDER BY CAST(order_date_period AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS channel_length_count_30d,
        MIN(amount) OVER (PARTITION BY status) AS status_min_amount
    FROM filtered
)
-- Aggregate with conditional metrics
SELECT
    status,
    CASE WHEN channel_length >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS channel_length_tier,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN status = 'PENDING' THEN 1 END) AS pending_count,
    APPROX_PERCENTILE(shipping_fee, 0.9) AS shipping_fee_p90,
    SUM(CASE WHEN status = 'COMPLETED' THEN amount ELSE 0 END) AS completed_amount,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids
FROM enriched
GROUP BY status, channel_length_tier
HAVING pending_count > 1;
