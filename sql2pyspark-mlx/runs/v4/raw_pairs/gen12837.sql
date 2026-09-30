WITH filtered AS (
    SELECT
        *,
        SUBSTRING(supplier_name, 1, 4) AS supplier_name_prefix,
        COALESCE(city, 'DIRECT') AS city_clean
    FROM suppliers
    WHERE rating >= 2.4
)
SELECT
    *,
    ROW_NUMBER() OVER w_recent AS event_seq,
    MIN(rating) OVER w_running AS city_min_rating,
    ROUND(rating / SUM(rating) OVER (PARTITION BY country), 4) AS rating_share,
    LEAD(rating, 1, 0) OVER w_ordered AS next_rating,
    AVG(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS rating_avg_5_prior
FROM filtered
WINDOW w_recent AS (PARTITION BY city ORDER BY rating, id), w_running AS (PARTITION BY city), w_ordered AS (ORDER BY rating, id)
