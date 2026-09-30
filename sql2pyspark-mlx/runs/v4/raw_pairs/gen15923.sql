WITH matched_keys AS (
    -- Set difference on keys
    SELECT city, segment
    FROM customers
    EXCEPT
    SELECT city, segment
    FROM customers
    WHERE MONTH(signup_date) IN (3, 9, 10)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    INNER JOIN matched_keys k ON s.city = k.city AND s.segment = k.segment
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        DENSE_RANK() OVER (ORDER BY age NULLS FIRST) AS age_dense_rank,
        CUME_DIST() OVER (ORDER BY age DESC) AS age_cume_dist,
        COUNT(age) OVER w_grp AS segment_count_age,
        CUME_DIST() OVER (ORDER BY credit_limit NULLS FIRST) AS credit_limit_cume_dist
    FROM kept_rows
    WINDOW w_grp AS (PARTITION BY segment, country)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions ch
            WHERE ch.customer_id = s.id
              AND ch.started_at <= s.signup_date
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, segment
    FROM kept
    EXCEPT DISTINCT
    SELECT id, segment
    FROM customers
    WHERE email IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.segment = k.segment
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN credit_limit >= 15400 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
        BIT_AND(age) AS age_bit_and,
        SUM(credit_limit_cume_dist) AS total_credit_limit_cume_dist,
        COUNT(*) FILTER (WHERE city = 'Chiang Mai') AS chiang_mai_rows
    FROM reconciled
    GROUP BY 1
)
SELECT credit_limit_tier, total_credit_limit_cume_dist
FROM daily
