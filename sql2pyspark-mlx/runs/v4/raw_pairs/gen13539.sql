WITH deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM suppliers
    QUALIFY RANK() OVER (PARTITION BY country, city ORDER BY rating DESC) = 1
),
latest_match AS (
    -- Latest matching product per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.category
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.category
            FROM products c
            WHERE c.supplier_id = s.id
            ORDER BY c.launch_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY category ORDER BY rating DESC, id) AS rating_bucket,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY rating DESC) AS rating_dense_rank,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
        SUM(rating) OVER (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating_2
    FROM latest_match
)
SELECT
    id,
    supplier_name,
    country,
    city,
    rating_bucket,
    cumulative_rating,
    CASE WHEN country RLIKE '^(JP|SG)$' THEN 'EXTENDED' WHEN country RLIKE '^(TH|US)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS country_zone
FROM metrics
ORDER BY rating_bucket DESC;
