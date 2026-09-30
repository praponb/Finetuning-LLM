WITH base AS (
    SELECT *, DATE_TRUNC('month', launch_date) AS launch_date_period
    FROM products
    WHERE launch_date > TO_DATE('2023-08-26')
      AND supplier_id IS NOT NULL
)
SELECT *
FROM (
    SELECT
        id,
        brand,
        price,
        cost
    FROM base
) src
UNPIVOT INCLUDE NULLS (
    metric_value FOR kpi IN (price AS PRICE_VAL, cost AS COST_VAL)
)
