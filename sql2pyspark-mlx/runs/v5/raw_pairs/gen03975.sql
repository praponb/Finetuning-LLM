WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        store_id,
        units,
        revenue,
        revenue - units AS revenue_per_unit
    FROM store_sales
    WHERE sale_date > TO_DATE('2023-09-14')
),
with_dims AS (
    -- Attach stores attributes
    SELECT
        s.id,
        s.store_id,
        s.units,
        s.revenue_per_unit,
        p.region,
        p.city
    FROM base s
    INNER JOIN stores p
        ON s.store_id = p.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(units) OVER (PARTITION BY city, store_id ORDER BY revenue_per_unit, id) AS prev_units,
        LAG(revenue_per_unit) OVER (PARTITION BY city, store_id ORDER BY revenue_per_unit, id) AS prev_revenue_per_unit,
        ROW_NUMBER() OVER (PARTITION BY city, store_id ORDER BY revenue_per_unit, id) AS event_seq,
        CUME_DIST() OVER (PARTITION BY city ORDER BY units DESC) AS units_cume_dist
    FROM with_dims
),
matched_keys AS (
    -- Set difference on keys
    SELECT store_id
    FROM with_windows
    EXCEPT
    SELECT store_id
    FROM store_sales
    WHERE sale_date <= '2023-08-01'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.store_id = k.store_id
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER w_hist AS revenue_per_unit_dense_rank,
        MIN(revenue_per_unit) OVER (PARTITION BY city) AS city_min_revenue_per_unit
    FROM kept_rows
    WINDOW w_hist AS (PARTITION BY store_id ORDER BY revenue_per_unit)
),
base_store_sales AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(revenue_per_unit_dense_rank * 0.9, 2) AS revenue_per_unit_dense_rank_scaled
    FROM scored
    WHERE region IS DISTINCT FROM 'North'
      AND revenue_per_unit_dense_rank < 8
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN revenue_per_unit_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_per_unit_dense_rank_tier,
    CASE WHEN revenue_per_unit_dense_rank_scaled >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END AS revenue_per_unit_dense_rank_scaled_tier,
    APPROX_PERCENTILE(prev_units, ARRAY(0.25, 0.5, 0.75)) AS prev_units_quartiles,
    APPROX_PERCENTILE(event_seq, ARRAY(0.25, 0.5, 0.75)) AS event_seq_quartiles,
    COUNT(*) AS num_rows
FROM base_store_sales
GROUP BY CASE WHEN revenue_per_unit_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN revenue_per_unit_dense_rank_scaled >= 77 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING num_rows > 1
