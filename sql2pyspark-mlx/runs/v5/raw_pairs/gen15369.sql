WITH enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        store_id,
        sale_date,
        CUME_DIST() OVER (PARTITION BY product_id, store_id ORDER BY revenue DESC) AS revenue_cume_dist,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY units DESC, id ASC) AS row_seq,
        CORR(revenue, units) OVER (PARTITION BY store_id) AS corr_revenue_units
    FROM store_sales
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(row_seq) OVER (PARTITION BY store_id ORDER BY sale_date ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_row_seq,
        AVG(row_seq) OVER (PARTITION BY store_id ORDER BY row_seq ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS row_seq_avg_5_prior,
        NTILE(5) OVER (PARTITION BY store_id ORDER BY revenue_cume_dist DESC, id ASC) AS revenue_cume_dist_bucket
    FROM enriched
    WHERE store_id IN (1, 2, 6)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, store_id
    FROM with_windows
    EXCEPT DISTINCT
    SELECT id, store_id
    FROM store_sales
    WHERE revenue BETWEEN 1100 AND 2600
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.store_id = k.store_id
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        src.*,
        b.region,
        b.opened_date,
        b.city
    FROM verified src
    LEFT JOIN stores b
        ON src.store_id = b.id
),
joined AS (
    -- Attach stores attributes
    SELECT e.*, d.city AS store_city
    FROM with_dims e
    INNER JOIN stores d
        ON e.store_id = d.id
)
SELECT
    id,
    store_id,
    sale_date,
    revenue_cume_dist,
    row_seq,
    corr_revenue_units,
    row_seq_avg_5_prior,
    revenue_cume_dist_bucket,
    region,
    CASE WHEN region RLIKE '^(West|East)$' THEN 'GROUP_B' WHEN region RLIKE '^(North|South)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS region_zone
FROM joined
