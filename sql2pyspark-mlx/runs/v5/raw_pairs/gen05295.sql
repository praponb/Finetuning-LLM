WITH remaining_keys AS (
    SELECT city
    FROM suppliers
    EXCEPT
    SELECT city
    FROM suppliers
    WHERE city != 'Berlin'
),
kept_rows AS (
    SELECT s.*
    FROM suppliers s
    INNER JOIN remaining_keys k ON s.city = k.city
),
scored AS (
    SELECT
        id,
        supplier_name,
        DENSE_RANK() OVER (ORDER BY rating DESC NULLS FIRST) AS rating_dense_rank
    FROM kept_rows
    WHERE supplier_name IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND x.supplier_id IS NOT NULL
        )
),
scored_suppliers AS (
    SELECT
        id,
        supplier_name,
        rating_dense_rank,
        CUME_DIST() OVER (ORDER BY rating_dense_rank NULLS FIRST) AS rating_dense_rank_cume_dist
    FROM kept
),
scored_suppliers_2 AS (
    SELECT
        *,
        PERCENT_RANK() OVER w_part AS rating_dense_rank_cume_dist_percent_rank,
        PERCENT_RANK() OVER w_part AS rating_dense_rank_cume_dist_percent_rank_2
    FROM scored_suppliers
    WINDOW w_part AS (ORDER BY rating_dense_rank_cume_dist DESC)
),
filtered AS (
    SELECT
        id,
        supplier_name,
        rating_dense_rank_cume_dist_percent_rank_2,
        UPPER(supplier_name) AS supplier_name_upper
    FROM scored_suppliers_2
    WHERE rating_dense_rank_cume_dist_percent_rank IS NULL
)
SELECT
    CASE WHEN rating_dense_rank_cume_dist_percent_rank_2 >= 0.78 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_dense_rank_cume_dist_percent_rank_2_tier,
    MIN(rating_dense_rank_cume_dist_percent_rank_2) AS min_rating_dense_rank_cume_dist_percent_rank_2,
    PERCENTILE_APPROX(rating_dense_rank_cume_dist_percent_rank_2, ARRAY(0.25, 0.5, 0.75)) AS rating_dense_rank_cume_dist_percent_rank_2_quartiles,
    ROUND(AVG(rating_dense_rank_cume_dist_percent_rank_2), 2) AS avg_rating_dense_rank_cume_dist_percent_rank_2,
    ROUND(AVG(rating_dense_rank_cume_dist_percent_rank_2), 2) AS avg_rating_dense_rank_cume_dist_percent_rank_2_2
FROM filtered
GROUP BY CASE WHEN rating_dense_rank_cume_dist_percent_rank_2 >= 0.78 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY rating_dense_rank_cume_dist_percent_rank_2_tier DESC NULLS LAST
