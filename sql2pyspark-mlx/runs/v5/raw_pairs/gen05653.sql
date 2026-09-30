WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(units * 0.85, 2) AS adjusted_units
    FROM store_sales
    WHERE product_id IN (3, 5, 8, 10)
),
enriched AS (
    -- Attach stores attributes
    SELECT
        a.*,
        b.sqft,
        b.opened_date
    FROM cleaned a
    JOIN stores b
        ON a.store_id = b.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(sqft) OVER (PARTITION BY product_id ORDER BY sqft, id) AS prev_sqft,
        LEAD(units) OVER (PARTITION BY product_id ORDER BY sqft, id) AS next_units,
        LAG(units) OVER (PARTITION BY product_id ORDER BY sqft, id) AS prev_units,
        ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY sqft, id) AS seq_no,
        DENSE_RANK() OVER (PARTITION BY product_id ORDER BY revenue NULLS FIRST) AS revenue_dense_rank
    FROM enriched
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN revenue >= 3500 THEN 'High' WHEN revenue >= 600 THEN 'Medium' ELSE 'Low' END AS revenue_band
    FROM with_windows
    WHERE adjusted_units < 23
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        store_id,
        adjusted_units,
        revenue
    FROM base
) src
UNPIVOT INCLUDE NULLS (
    value FOR kpi IN (adjusted_units, revenue)
);
