WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        supplier_name,
        country,
        rating
    FROM suppliers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        supplier_name,
        country,
        MIN(rating) OVER w_ordered AS running_min_rating,
        SUM(rating) OVER w_ordered AS running_sum_rating,
        COUNT(rating) OVER w_ordered AS running_count_rating
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY country ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(running_min_rating) OVER (ORDER BY running_min_rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_min_rating,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY running_count_rating, id) AS seq_no
    FROM metrics
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND LENGTH(ch.product_name) >= 7
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(seq_no AS STRING) AS seq_no_str
    FROM eligible
    WHERE country IN ('TH', 'DE')
)
SELECT
    id,
    country,
    running_min_rating,
    running_sum_rating,
    running_count_rating,
    seq_no,
    seq_no_str,
    CASE WHEN country RLIKE '^(TH|UK)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS country_zone
FROM cleaned;
