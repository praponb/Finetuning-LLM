WITH enriched AS (
    SELECT
        *,
        AVG(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS rating_avg_6_prior,
        CUME_DIST() OVER (PARTITION BY country ORDER BY rating NULLS LAST) AS rating_cume_dist,
        AVG(rating) OVER w_running AS rating_avg_5_prior,
        NTILE(3) OVER (PARTITION BY country, city ORDER BY rating DESC, id) AS rating_bucket
    FROM suppliers
    WINDOW w_running AS (ORDER BY rating, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
)
SELECT
    id,
    supplier_name,
    rating,
    rating_cume_dist,
    rating_avg_5_prior,
    rating_bucket,
    CASE WHEN supplier_name RLIKE '^(Siam Parts Co|Pacific Wholesale)$' THEN 'CORE' ELSE 'GROUP_B' END AS supplier_name_zone
FROM enriched
ORDER BY supplier_name_zone NULLS LAST, rating_bucket
