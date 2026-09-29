WITH valid AS (
    SELECT
        *,
        units * revenue AS revenue_per_unit,
        ROUND(units * 1.1, 2) AS units_scaled
    FROM store_sales
    WHERE sale_date >= DATE '2023-10-01'
),
matched_keys AS (
    SELECT product_id
    FROM valid
    EXCEPT
    SELECT product_id
    FROM store_sales
    WHERE units <> 35
),
kept_rows AS (
    SELECT s.*
    FROM valid s
    INNER JOIN matched_keys k ON s.product_id = k.product_id
),
base AS (
    SELECT *, ROUND(revenue_per_unit, 1) AS revenue_per_unit_rounded
    FROM kept_rows
    WHERE sale_date > DATE '2024-03-13'
),
daily AS (
    SELECT
        store_id,
        AVG(units) AS avg_units,
        COUNT(*) AS row_count,
        COUNT(*) AS num_rows
    FROM base
    GROUP BY store_id
)
SELECT
    store_id,
    row_count,
    num_rows
FROM daily
ORDER BY row_count DESC NULLS FIRST;
