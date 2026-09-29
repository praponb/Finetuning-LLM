WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(rating) OVER (PARTITION BY city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_rating,
        ROW_NUMBER() OVER (PARTITION BY city, country ORDER BY rating DESC, id) AS row_seq,
        SUM(rating) OVER (PARTITION BY country, city ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating,
        SUM(rating) OVER (ORDER BY rating, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_rating_2
    FROM suppliers
    WHERE LENGTH(supplier_name) > 7
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating DESC, id) AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY cumulative_rating, id) AS seq_no
    FROM enriched
),
remaining_keys AS (
    -- Set difference on keys
    SELECT country, city
    FROM with_windows
    EXCEPT
    SELECT country, city
    FROM suppliers
    WHERE country = 'UK'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.country = k.country AND s.city = k.city
),
with_windows_suppliers AS (
    -- Running and moving aggregates
    SELECT
        id,
        city,
        rating,
        cumulative_rating,
        row_seq,
        running_rating,
        event_seq,
        seq_no,
        ROUND(cumulative_rating / SUM(cumulative_rating) OVER (PARTITION BY country), 4) AS cumulative_rating_share
    FROM kept_rows
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows_suppliers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM products ch
            WHERE ch.supplier_id = s.id
              AND ch.supplier_id IS NOT NULL
        )
),
with_windows_suppliers_2 AS (
    -- Window metrics over several different partitions
    SELECT
        rating,
        running_rating,
        event_seq,
        cumulative_rating_share,
        ROUND(rating / SUM(rating) OVER (PARTITION BY city), 4) AS rating_share,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY city), 4) AS seq_no_share,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY city), 4) AS seq_no_share_2,
        ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY city), 4) AS seq_no_share_3,
        AVG(running_rating) OVER (PARTITION BY city) AS city_avg_running_rating
    FROM not_flagged
),
with_windows_suppliers_3 AS (
    -- Window metrics over several different partitions
    SELECT *, PERCENT_RANK() OVER (ORDER BY seq_no_share_2 DESC NULLS FIRST) AS seq_no_share_2_percent_rank
    FROM with_windows_suppliers_2
)
-- Multi-level aggregation
SELECT
    CASE WHEN seq_no_share >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END AS seq_no_share_tier,
    APPROX_PERCENTILE(rating_share, 0.95) AS rating_share_p95,
    ROUND(AVG(city_avg_running_rating), 2) AS avg_city_avg_running_rating,
    PERCENTILE_APPROX(rating, 0.95) AS rating_p95,
    ROUND(AVG(seq_no_share_3), 2) AS avg_seq_no_share_3
FROM with_windows_suppliers_3
GROUP BY CASE WHEN seq_no_share >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING PERCENTILE_APPROX(rating, 0.95) >= 0;
