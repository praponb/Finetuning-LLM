WITH matched_keys AS (
    -- Set difference on keys
    SELECT product_id, store_id
    FROM store_sales
    EXCEPT DISTINCT
    SELECT product_id, store_id
    FROM store_sales
    WHERE product_id IN (5, 6, 10)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM store_sales s
    JOIN matched_keys k ON s.product_id = k.product_id AND s.store_id = k.store_id
),
joined AS (
    -- Attach stores attributes
    SELECT s.*, d.store_name
    FROM verified s
    LEFT JOIN stores d
        ON s.store_id = d.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        store_id,
        sale_date,
        store_name,
        GREATEST(units, revenue) AS greatest_units_revenue,
        UPPER(TRIM(store_name)) AS store_name_normalized
    FROM joined
    WHERE sale_date > TO_DATE('2024-04-05')
      AND store_id IN (3, 4, 5)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY greatest_units_revenue DESC) AS greatest_units_revenue_percent_rank,
        AVG(greatest_units_revenue) OVER w_hist AS store_id_avg_greatest_units_revenue,
        PERCENT_RANK() OVER w_ordered AS greatest_units_revenue_percent_rank_2,
        MAX(greatest_units_revenue) OVER w_hist AS store_id_max_greatest_units_revenue,
        SUM(greatest_units_revenue) OVER w_hist AS store_id_sum_greatest_units_revenue
    FROM base
    WINDOW w_hist AS (PARTITION BY store_id), w_ordered AS (ORDER BY greatest_units_revenue NULLS LAST)
),
with_dims AS (
    -- Attach stores attributes
    SELECT e.*, p.city
    FROM metrics e
    JOIN stores p
        ON e.store_id = p.id
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        store_id,
        city,
        greatest_units_revenue_percent_rank
    FROM with_dims
) src
PIVOT (
    SUM(greatest_units_revenue_percent_rank) AS total
    FOR city IN ('Berlin', 'Bangkok')
)
