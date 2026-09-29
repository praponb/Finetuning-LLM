WITH eligible AS (
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND UPPER(TRIM(ch.product_name)) = 'OFFICE CHAIR'
        )
),
valid AS (
    SELECT
        id,
        supplier_name,
        city,
        rating,
        INITCAP(TRIM(country)) AS country_title
    FROM eligible
    WHERE city IN ('Chiang Mai', 'New York', 'Bangkok')
),
with_windows AS (
    SELECT
        id,
        supplier_name,
        rating,
        MIN(rating) OVER w_part AS running_min_rating,
        COUNT(rating) OVER w_part AS running_count_rating,
        SUM(rating) OVER w_part AS running_sum_rating,
        PERCENT_RANK() OVER (PARTITION BY city ORDER BY rating DESC) AS rating_percent_rank
    FROM valid
    WINDOW w_part AS (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    SELECT id
    FROM with_windows
    EXCEPT ALL
    SELECT id
    FROM suppliers
    WHERE supplier_name LIKE 'Pa%'
),
reconciled AS (
    SELECT s.*
    FROM with_windows s
    JOIN kept_keys k ON s.id = k.id
),
remaining_keys AS (
    SELECT id
    FROM reconciled
    INTERSECT
    SELECT id
    FROM suppliers
    WHERE supplier_name LIKE 'Eur%'
),
reconciled_suppliers AS (
    SELECT s.*
    FROM reconciled s
    JOIN remaining_keys k ON s.id = k.id
),
eligible_suppliers AS (
    SELECT s.*
    FROM reconciled_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND ARRAY_CONTAINS(c.tags, 'premium')
        )
),
with_windows_suppliers AS (
    SELECT
        rating,
        running_count_rating,
        running_sum_rating,
        rating_percent_rank,
        PERCENT_RANK() OVER (ORDER BY rating NULLS FIRST) AS rating_percent_rank_2,
        RANK() OVER (ORDER BY running_sum_rating NULLS FIRST) AS running_sum_rating_rank
    FROM eligible_suppliers
)
SELECT
    CASE WHEN running_count_rating >= 15 THEN 'HIGH' ELSE 'LOW' END AS running_count_rating_tier,
    PERCENTILE_APPROX(rating_percent_rank_2, ARRAY(0.25, 0.5, 0.75)) AS rating_percent_rank_2_quartiles,
    APPROX_PERCENTILE(running_count_rating, 0.5) AS running_count_rating_p50
FROM with_windows_suppliers
GROUP BY 1
ORDER BY running_count_rating_tier
