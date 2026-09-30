WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        store_name,
        city,
        LAG(sqft) OVER (PARTITION BY city ORDER BY opened_date, id) AS prev_sqft,
        LEAD(sqft) OVER (PARTITION BY city ORDER BY opened_date, id) AS next_sqft,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY opened_date, id) AS row_seq,
        AVG(sqft) OVER (PARTITION BY city ORDER BY opened_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS sqft_avg_6_prior
    FROM stores
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM inventory ch
            WHERE ch.store_id = s.id
              AND ch.quantity < 75
        )
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY city ORDER BY row_seq DESC, id) AS row_num
        FROM eligible
    ) x
    WHERE row_num = 1
),
scored_stores AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(prev_sqft) OVER w_ordered AS running_max_prev_sqft,
        COUNT(prev_sqft) OVER w_ordered AS running_count_prev_sqft,
        AVG(prev_sqft) OVER w_ordered AS running_avg_prev_sqft
    FROM latest
    WINDOW w_ordered AS (PARTITION BY city ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
totals AS (
    -- Grouped metrics
    SELECT
        city,
        COUNT(*) AS cnt,
        PERCENTILE_APPROX(running_count_prev_sqft, ARRAY(0.25, 0.5, 0.75)) AS running_count_prev_sqft_quartiles,
        SUM(CASE WHEN city = 'Berlin' THEN running_count_prev_sqft ELSE 0 END) AS berlin_running_count_prev_sqft,
        MAX(next_sqft) FILTER (WHERE city = 'New York') AS new_york_max_next_sqft
    FROM scored_stores
    GROUP BY city
    HAVING SUM(CASE WHEN city = 'Berlin' THEN running_count_prev_sqft ELSE 0 END) < 1000000
)
SELECT
    city,
    running_count_prev_sqft_quartiles,
    berlin_running_count_prev_sqft,
    new_york_max_next_sqft,
    CASE WHEN city RLIKE '^(New York|Bangkok)$' THEN 'CORE' WHEN city RLIKE '^(Chiang Mai|Singapore)$' THEN 'PRIORITY' ELSE 'GROUP_B' END AS city_zone
FROM totals
ORDER BY city DESC NULLS LAST;
