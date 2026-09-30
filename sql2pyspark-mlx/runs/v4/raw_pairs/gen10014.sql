WITH latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY cost DESC NULLS LAST, id) AS latest_rank
        FROM products
    ) t
    WHERE latest_rank = 1
)
SELECT
    category,
    STDDEV_SAMP(stock_qty) AS stock_qty_stddev,
    SUM(cost) AS total_cost,
    COUNT(DISTINCT brand) AS distinct_brands
FROM latest
GROUP BY category
