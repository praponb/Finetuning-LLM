WITH valid AS (
    SELECT *, TRIM(supplier_name) AS clean_supplier_name
    FROM suppliers
    WHERE city IS NOT NULL
)
SELECT
    *,
    ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share,
    CUME_DIST() OVER (PARTITION BY city ORDER BY rating DESC NULLS FIRST) AS rating_cume_dist
FROM valid;
