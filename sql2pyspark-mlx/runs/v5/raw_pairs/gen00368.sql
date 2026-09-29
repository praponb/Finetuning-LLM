WITH remaining_keys AS (
    -- Set intersection on keys
    SELECT id, country
    FROM customers
    INTERSECT
    SELECT id, country
    FROM customers
    WHERE UPPER(email) LIKE '%ACK%'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM customers s
    JOIN remaining_keys k ON s.id = k.id AND s.country = k.country
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at <= s.signup_date
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit + age AS credit_limit_plus_age
    FROM kept
    WHERE segment IS NOT NULL
      AND city IS DISTINCT FROM 'Singapore'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        name,
        segment,
        is_active,
        DENSE_RANK() OVER (ORDER BY credit_limit_plus_age DESC NULLS FIRST) AS credit_limit_plus_age_dense_rank
    FROM base
    WHERE city IN ('London', 'Berlin', 'Tokyo')
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        segment,
        COUNT(DISTINCT segment) AS distinct_segments,
        SUM(CASE WHEN segment = 'SMB' THEN credit_limit_plus_age_dense_rank ELSE 0 END) AS smb_credit_limit_plus_age_dense_rank,
        ROUND(SUM(credit_limit_plus_age_dense_rank) / NULLIF(COUNT(CASE WHEN segment = 'SMB' THEN 1 END), 0), 2) AS credit_limit_plus_age_dense_rank_per_smb,
        COUNT(DISTINCT segment) AS distinct_segments_2,
        COUNT(*) AS num_rows
    FROM metrics
    GROUP BY segment
)
SELECT
    segment,
    distinct_segments,
    smb_credit_limit_plus_age_dense_rank,
    credit_limit_plus_age_dense_rank_per_smb,
    distinct_segments_2,
    num_rows
FROM stats;
