WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, DATE_SUB(order_date, 63) AS order_date_minus_63d
    FROM orders
    WHERE order_date >= TO_DATE('2023-10-17')
)
-- Keep one row per key (latest first)
SELECT *
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY order_date_minus_63d, id) AS latest_rank
    FROM cleaned
) t
WHERE latest_rank = 1;
