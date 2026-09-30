WITH filtered AS (
    SELECT *, UPPER(segment) AS segment_upper
    FROM customers
    WHERE signup_date > '2023-08-01'
),
with_windows AS (
    SELECT
        id,
        email,
        segment,
        age,
        NTILE(3) OVER (PARTITION BY country ORDER BY age ASC, id ASC) AS age_bucket,
        AVG(credit_limit) OVER (PARTITION BY city) AS city_avg_credit_limit
    FROM filtered
    WHERE city IN ('Berlin', 'Tokyo')
),
with_windows_customers AS (
    SELECT
        id,
        age,
        city_avg_credit_limit,
        MIN(age) OVER (PARTITION BY segment ORDER BY age_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_age,
        AVG(age) OVER (PARTITION BY segment ORDER BY age_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_age,
        COUNT(age) OVER (PARTITION BY segment ORDER BY age_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_age,
        MAX(age) OVER (PARTITION BY segment ORDER BY age_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_age,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY city_avg_credit_limit ASC, id ASC) AS event_seq
    FROM with_windows
),
enriched AS (
    SELECT
        id,
        age,
        running_avg_age,
        running_count_age,
        running_max_age,
        ROW_NUMBER() OVER (ORDER BY city_avg_credit_limit ASC, id ASC) AS seq_no,
        SUM(city_avg_credit_limit) OVER (ORDER BY running_count_age ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_city_avg_credit_limit,
        NTILE(4) OVER (ORDER BY running_max_age ASC, id ASC) AS running_max_age_bucket
    FROM with_windows_customers
),
eligible AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.customer_id IS NOT NULL
        )
),
eligible_customers AS (
    SELECT s.*
    FROM eligible s
    WHERE EXISTS (
            SELECT 1
            FROM subscriptions ch
            WHERE ch.customer_id = s.id
              AND ch.customer_id IS NOT NULL
        )
),
remaining_keys AS (
    SELECT id
    FROM eligible_customers
    EXCEPT
    SELECT id
    FROM customers
    WHERE MONTH(signup_date) IN (7, 8, 10)
),
reconciled AS (
    SELECT s.*
    FROM eligible_customers s
    INNER JOIN remaining_keys k ON s.id = k.id
)
SELECT
    id,
    age,
    running_avg_age,
    running_count_age,
    running_max_age,
    seq_no,
    running_city_avg_credit_limit,
    running_max_age_bucket
FROM reconciled
ORDER BY running_max_age_bucket ASC NULLS LAST
