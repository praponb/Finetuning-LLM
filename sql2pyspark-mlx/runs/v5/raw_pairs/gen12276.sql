WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        CAST(units AS DOUBLE) AS units_str
    FROM store_sales
    WHERE sale_date > DATE '2023-10-06'
      AND revenue <= 1700
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units_str,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id, store_id), 4) AS revenue_share,
        ROUND(revenue / SUM(revenue) OVER (PARTITION BY product_id), 4) AS revenue_share_2,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY units DESC NULLS FIRST) AS units_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY units_str, id) AS event_seq,
        AVG(units) OVER (ORDER BY units, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS units_avg_2
    FROM cleaned
    WHERE revenue >= 3800
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        product_id,
        revenue_share_2,
        event_seq,
        units_avg_2,
        SUM(event_seq) OVER w_recent AS running_sum_event_seq,
        AVG(event_seq) OVER w_recent AS running_avg_event_seq,
        COUNT(event_seq) OVER w_recent AS running_count_event_seq,
        RANK() OVER (PARTITION BY store_id ORDER BY revenue_share_2) AS revenue_share_2_rank
    FROM enriched
    WINDOW w_recent AS (PARTITION BY store_id, product_id ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
joined AS (
    -- Attach products attributes
    SELECT a.*, d.product_name
    FROM with_windows a
    LEFT JOIN products d
        ON a.product_id = d.id
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY product_id ORDER BY running_count_event_seq) AS dedup_rank
        FROM joined
    ) x
    WHERE dedup_rank = 1
)
SELECT
    s.id,
    s.product_id,
    s.revenue_share_2,
    s.event_seq,
    s.units_avg_2,
    s.running_sum_event_seq,
    s.running_avg_event_seq,
    s.revenue_share_2_rank,
    (SELECT SUM(o2.quantity) FROM order_items o2 WHERE o2.product_id = s.product_id) AS sum_order_item_quantity
FROM latest s
ORDER BY revenue_share_2 DESC
