WITH valid AS (
    -- Keep recent, valid rows
    SELECT *, COALESCE(shipping_fee, 0) AS shipping_fee_filled
    FROM orders
    WHERE customer_id IS NOT NULL
      AND channel IS DISTINCT FROM 'web'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(shipping_fee) OVER (PARTITION BY store_id ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_shipping_fee,
        MAX(shipping_fee) OVER (PARTITION BY store_id ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_shipping_fee,
        MIN(shipping_fee) OVER (PARTITION BY store_id ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_shipping_fee,
        COUNT(shipping_fee) OVER (PARTITION BY store_id ORDER BY discount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_shipping_fee,
        ROW_NUMBER() OVER (PARTITION BY channel ORDER BY shipping_fee_filled ASC, id ASC) AS event_seq
    FROM valid
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_count_shipping_fee ASC NULLS LAST, id ASC) AS row_num
        FROM scored
    ) x
    WHERE row_num = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        amount,
        discount,
        shipping_fee,
        shipping_fee_filled,
        running_avg_shipping_fee,
        running_max_shipping_fee,
        running_min_shipping_fee,
        running_count_shipping_fee,
        event_seq,
        row_num,
        CASE WHEN shipping_fee >= 36 THEN 'High' WHEN shipping_fee >= 15 THEN 'Medium' ELSE 'Low' END AS shipping_fee_tier,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM ranked
    WHERE running_max_shipping_fee >= 29
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        store_id,
        discount,
        shipping_fee,
        running_max_shipping_fee,
        running_min_shipping_fee,
        running_count_shipping_fee,
        event_seq,
        row_num,
        shipping_fee_tier,
        resolved_channel,
        LEAD(shipping_fee) OVER w_hist AS next_shipping_fee,
        LEAD(running_max_shipping_fee) OVER w_hist AS next_running_max_shipping_fee,
        LAG(running_max_shipping_fee) OVER w_hist AS prev_running_max_shipping_fee,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM base
    WINDOW w_hist AS (PARTITION BY store_id ORDER BY running_count_shipping_fee ASC, id ASC)
)
-- Join a dimension and aggregate
SELECT
    dim.region,
    COUNT(*) AS cnt,
    ROUND(SUM(a.next_shipping_fee), 2) AS total_next_shipping_fee,
    MIN(a.discount) AS min_discount
FROM enriched a
INNER JOIN stores dim ON a.store_id = dim.id
GROUP BY ROLLUP (dim.region)
ORDER BY min_discount ASC NULLS FIRST;
