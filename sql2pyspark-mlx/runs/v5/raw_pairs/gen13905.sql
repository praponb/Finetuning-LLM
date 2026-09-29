WITH eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products x
            WHERE x.supplier_id = s.id
              AND LENGTH(x.product_name) < 9
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        supplier_name,
        country,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        LEAD(rating, 2, 0) OVER (PARTITION BY city ORDER BY rating, id) AS next_rating
    FROM eligible
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND ARRAY_CONTAINS(c.tags, 'premium')
        )
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY country ORDER BY running_rating DESC) AS row_num
        FROM not_flagged
    ) t
    WHERE row_num = 1
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE EXISTS (
            SELECT 1
            FROM products c
            WHERE c.supplier_id = s.id
              AND c.supplier_id IN (8, 12)
        )
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        supplier_name,
        country,
        next_rating,
        row_num,
        next_rating * running_rating AS next_rating_times_running_rating
    FROM kept
    WHERE LOWER(TRIM(supplier_name)) = 'lion city imports'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(next_rating_times_running_rating) IGNORE NULLS OVER w_ordered AS last_known_next_rating_times_running_rating,
        LAST_VALUE(next_rating_times_running_rating, TRUE) OVER (PARTITION BY country ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_next_rating_times_running_rating_2,
        NTILE(3) OVER w_hist AS next_rating_bucket,
        ROW_NUMBER() OVER w_hist AS seq_no,
        NTILE(3) OVER (PARTITION BY country ORDER BY next_rating_times_running_rating, id) AS next_rating_times_running_rating_bucket
    FROM valid
    WINDOW w_ordered AS (ORDER BY row_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY country ORDER BY next_rating DESC, id)
),
totals AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN last_known_next_rating_times_running_rating >= 75 THEN 'LARGE' ELSE 'SMALL' END AS last_known_next_rating_times_running_rating_tier,
        seq_no,
        CASE WHEN next_rating_bucket >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_rating_bucket_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list,
        ROUND(AVG(seq_no), 2) AS avg_seq_no
    FROM scored
    GROUP BY last_known_next_rating_times_running_rating_tier, seq_no, next_rating_bucket_tier
    HAVING COUNT(DISTINCT id) >= 1
)
SELECT
    last_known_next_rating_times_running_rating_tier,
    seq_no,
    next_rating_bucket_tier,
    distinct_ids,
    country_list,
    avg_seq_no
FROM totals
ORDER BY avg_seq_no DESC, next_rating_bucket_tier
