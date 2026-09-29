WITH filtered AS (
    SELECT *, DATE_ADD(sale_date, 19) AS sale_date_plus_19d
    FROM store_sales
    WHERE sale_date >= DATE '2023-12-26'
      AND units = 27
),
enriched AS (
    SELECT e.*, s.sqft
    FROM filtered e
    LEFT JOIN stores s
        ON e.store_id = s.id
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sqft DESC NULLS LAST, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
latest_store_sales AS (
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY store_id ORDER BY sale_date DESC NULLS LAST, id) = 1
)
SELECT
    id,
    store_id,
    product_id,
    sale_date,
    revenue,
    sale_date_plus_19d,
    sqft,
    rank_in_group
FROM latest_store_sales
ORDER BY id, sale_date_plus_19d DESC
LIMIT 20;
