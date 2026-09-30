WITH with_windows AS (
    SELECT
        id,
        store_id,
        product_id,
        sale_date,
        units,
        revenue,
        PERCENT_RANK() OVER (PARTITION BY product_id ORDER BY units NULLS LAST) AS units_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY store_id, product_id ORDER BY sale_date DESC, id) AS row_seq,
        AVG(revenue) OVER (PARTITION BY store_id) AS store_id_avg_revenue
    FROM store_sales
),
cleaned AS (
    SELECT
        id,
        store_id,
        units,
        revenue,
        units_percent_rank,
        row_seq,
        store_id_avg_revenue,
        CASE WHEN units >= 41 THEN 'High' WHEN units >= 15 THEN 'Medium' ELSE 'Low' END AS units_level
    FROM with_windows
    WHERE sale_date > DATE '2024-02-22'
),
matched_keys AS (
    SELECT store_id
    FROM cleaned
    INTERSECT
    SELECT store_id
    FROM store_sales
    WHERE units < 37
),
verified AS (
    SELECT s.*
    FROM cleaned s
    JOIN matched_keys k ON s.store_id = k.store_id
)
SELECT
    CASE WHEN row_seq >= 9 THEN 'HIGH' ELSE 'LOW' END AS row_seq_tier,
    store_id,
    APPROX_PERCENTILE(row_seq, 0.95) AS row_seq_p95,
    PERCENTILE_APPROX(units_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS units_percent_rank_quartiles
FROM verified
GROUP BY row_seq_tier, store_id
ORDER BY store_id DESC NULLS LAST;
