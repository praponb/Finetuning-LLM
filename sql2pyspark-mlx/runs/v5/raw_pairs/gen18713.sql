WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT *, LENGTH(segment) AS segment_length
    FROM customers
    WHERE signup_date > DATE '2023-06-01'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        city,
        is_active,
        credit_limit,
        age,
        segment_length,
        NTILE(5) OVER w_grp AS age_bucket,
        LEAD(credit_limit, 1) OVER (PARTITION BY segment, city ORDER BY segment_length, id) AS next_credit_limit,
        NTILE(10) OVER w_running AS age_bucket_2,
        NTILE(10) OVER (PARTITION BY segment ORDER BY credit_limit DESC, id) AS credit_limit_bucket
    FROM cleaned
    WINDOW w_grp AS (PARTITION BY segment, country ORDER BY age DESC, id), w_running AS (PARTITION BY segment ORDER BY age DESC, id)
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(age) OVER (PARTITION BY city ORDER BY credit_limit_bucket, id) AS prev_age,
        LEAD(age) OVER (PARTITION BY city ORDER BY credit_limit_bucket, id) AS next_age,
        LAG(next_credit_limit) OVER (PARTITION BY city ORDER BY credit_limit_bucket, id) AS prev_next_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY credit_limit_bucket, id) AS seq_no,
        ROUND(age / SUM(age) OVER (PARTITION BY city), 4) AS age_share
    FROM enriched
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM scored s
    WHERE EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.browser != 'Safari'
        )
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id, city
    FROM not_flagged
    INTERSECT ALL
    SELECT id, city
    FROM customers
    WHERE segment IN ('Corporate', 'Consumer', 'SMB')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM not_flagged s
    INNER JOIN kept_keys k ON s.id = k.id AND s.city = k.city
),
with_windows AS (
    -- Running and moving aggregates
    SELECT *, ROUND(seq_no / SUM(seq_no) OVER (PARTITION BY city), 4) AS seq_no_share
    FROM verified
)
SELECT
    city,
    is_active,
    segment_length,
    age_bucket,
    next_credit_limit,
    credit_limit_bucket,
    prev_age,
    seq_no,
    seq_no_share
FROM with_windows
ORDER BY seq_no DESC NULLS LAST, segment_length
