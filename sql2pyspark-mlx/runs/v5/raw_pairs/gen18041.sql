WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        name,
        email,
        country,
        city,
        is_active,
        age,
        LENGTH(email) AS email_length
    FROM customers
    WHERE signup_date > TO_DATE('2023-03-21')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        email,
        country,
        city,
        AVG(email_length) OVER w_running AS city_avg_email_length,
        FIRST_VALUE(city, TRUE) OVER w_hist AS first_known_city,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY age DESC, id) AS seq_no,
        SUM(email_length) OVER w_hist AS running_email_length
    FROM base
    WHERE country != 'TH'
    WINDOW w_running AS (PARTITION BY city), w_hist AS (PARTITION BY country ORDER BY email_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        email,
        city,
        seq_no,
        NTILE(4) OVER w_part AS seq_no_bucket,
        LAG(seq_no, 1) OVER w_hist AS prev_seq_no
    FROM metrics
    WINDOW w_part AS (ORDER BY seq_no DESC, id), w_hist AS (PARTITION BY country ORDER BY seq_no, id)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *, RANK() OVER (PARTITION BY city ORDER BY seq_no_bucket DESC) AS latest_rank
    FROM scored
    QUALIFY latest_rank = 1
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.end_date >= '2025-09-01'
        )
),
not_flagged_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM not_flagged s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date >= '2023-03-27'
        )
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        seq_no_bucket,
        latest_rank,
        LEAD(seq_no_bucket) OVER w_part AS next_seq_no_bucket,
        LAG(prev_seq_no) OVER w_part AS prev_prev_seq_no,
        ROW_NUMBER() OVER w_part AS row_seq
    FROM not_flagged_customers
    WINDOW w_part AS (PARTITION BY city ORDER BY seq_no_bucket, id)
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        city,
        COUNT_IF(city = 'Berlin') AS berlin_events,
        SUM(prev_prev_seq_no) FILTER (WHERE city = 'New York') AS new_york_total_prev_prev_seq_no,
        SORT_ARRAY(COLLECT_SET(city)) AS city_list,
        COUNT(*) AS event_count
    FROM enriched
    GROUP BY city
)
SELECT
    city,
    berlin_events,
    new_york_total_prev_prev_seq_no,
    city_list,
    event_count,
    CASE WHEN city RLIKE '^(Berlin|Singapore)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS city_zone
FROM stats
ORDER BY new_york_total_prev_prev_seq_no DESC
