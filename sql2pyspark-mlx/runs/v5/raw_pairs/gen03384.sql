WITH filtered AS (
    SELECT *, CEIL(units) AS units_whole
    FROM store_sales
    WHERE sale_date >= DATE '2023-08-10'
      AND revenue > 2900
),
with_dims AS (
    SELECT
        e.id,
        e.store_id,
        e.product_id,
        e.sale_date,
        p.stock_qty,
        p.price
    FROM filtered e
    LEFT JOIN products p
        ON e.product_id = p.id
),
metrics AS (
    SELECT
        *,
        AVG(price) OVER (PARTITION BY store_id ORDER BY price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS price_avg_6,
        MIN(stock_qty) OVER (PARTITION BY store_id) AS store_id_min_stock_qty,
        SUM(price) OVER (PARTITION BY product_id ORDER BY sale_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_price
    FROM with_dims
),
with_windows AS (
    SELECT
        id,
        sale_date,
        price_avg_6,
        store_id_min_stock_qty,
        LEAD(price, 2) OVER (PARTITION BY product_id, store_id ORDER BY cumulative_price, id) AS next_price,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY cumulative_price NULLS FIRST) AS cumulative_price_percent_rank,
        NTILE(5) OVER (ORDER BY price_avg_6, id) AS price_avg_6_bucket,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sale_date, id) AS event_seq
    FROM metrics
),
scored AS (
    SELECT
        id,
        store_id_min_stock_qty,
        next_price,
        price_avg_6_bucket,
        event_seq,
        AVG(cumulative_price_percent_rank) OVER w_part AS last7_avg_cumulative_price_percent_rank,
        SUM(cumulative_price_percent_rank) OVER w_part AS last7_sum_cumulative_price_percent_rank,
        COUNT(cumulative_price_percent_rank) OVER w_part AS last7_count_cumulative_price_percent_rank
    FROM with_windows
    WINDOW w_part AS (ORDER BY next_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
)
SELECT
    CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
    CASE WHEN last7_avg_cumulative_price_percent_rank >= 0.8 THEN 'PREMIUM' ELSE 'STANDARD' END AS last7_avg_cumulative_price_percent_rank_tier,
    PERCENTILE_APPROX(next_price, ARRAY(0.25, 0.5, 0.75)) AS next_price_quartiles,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(last7_sum_cumulative_price_percent_rank, 0.95) AS last7_sum_cumulative_price_percent_rank_p95
FROM scored
GROUP BY CUBE (CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN last7_avg_cumulative_price_percent_rank >= 0.8 THEN 'PREMIUM' ELSE 'STANDARD' END);
