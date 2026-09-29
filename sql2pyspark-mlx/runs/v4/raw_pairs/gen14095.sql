WITH filtered AS (
    SELECT
        id,
        units,
        revenue,
        CAST(units AS DOUBLE) AS units_num
    FROM store_sales
    WHERE sale_date >= DATE '2023-10-18'
)
SELECT
    *,
    LAST_VALUE(units_num, TRUE) OVER (ORDER BY units_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_units_num,
    DENSE_RANK() OVER (ORDER BY units DESC) AS units_dense_rank
FROM filtered
