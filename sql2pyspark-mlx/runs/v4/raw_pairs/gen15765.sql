WITH joined AS (
    -- Attach suppliers attributes
    SELECT
        src.*,
        p.supplier_name,
        p.rating,
        p.city
    FROM products src
    JOIN suppliers p
        ON src.supplier_id = p.id
)
-- Multi-level aggregation
SELECT
    city,
    COUNT(DISTINCT brand) AS distinct_brands,
    SUM(CASE WHEN city = 'Tokyo' THEN rating ELSE 0 END) AS tokyo_rating,
    BIT_AND(stock_qty) AS stock_qty_bit_and
FROM joined
GROUP BY 1
HAVING tokyo_rating >= 0;
