SELECT
    *,
    CUME_DIST() OVER (PARTITION BY country ORDER BY rating NULLS FIRST) AS rating_cume_dist,
    LAST_VALUE(city, TRUE) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_city
FROM suppliers
