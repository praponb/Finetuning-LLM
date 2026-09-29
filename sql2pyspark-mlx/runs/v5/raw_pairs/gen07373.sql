WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        CASE WHEN country = 'UK' THEN 1 ELSE 0 END AS is_uk,
        LEAST(age, credit_limit) AS least_age_credit_limit,
        COALESCE(city, 'N/A') AS resolved_city
    FROM customers
    WHERE signup_date >= DATE '2023-06-20'
      AND segment IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(age) OVER w_running AS next_age,
        LEAD(credit_limit) OVER w_running AS next_credit_limit,
        ROW_NUMBER() OVER w_running AS row_seq
    FROM base
    WHERE segment != 'Corporate'
    WINDOW w_running AS (PARTITION BY country, segment ORDER BY is_uk ASC, id ASC)
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.customer_id IS NOT NULL
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts x
            WHERE x.customer_id = s.id
              AND x.opened_date >= '2025-03-01'
        )
),
kept_customers AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM kept s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.channel != 'partner'
        )
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        least_age_credit_limit,
        next_age,
        next_credit_limit,
        row_seq,
        CASE WHEN city = 'London' THEN 1 ELSE 0 END AS is_london,
        ROUND(next_age * 0.9, 2) AS next_age_adjusted
    FROM kept_customers
    WHERE segment IS NOT NULL
      AND age < 49
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN next_credit_limit >= 15900 THEN 'HIGH' ELSE 'LOW' END AS next_credit_limit_tier,
        CASE WHEN next_age_adjusted >= 77 THEN 'HIGH' ELSE 'LOW' END AS next_age_adjusted_tier,
        AVG(next_age_adjusted) FILTER (WHERE country = 'UK') AS uk_avg_next_age_adjusted,
        COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count,
        ROUND(AVG(row_seq), 2) AS avg_row_seq,
        COUNT(*) FILTER (WHERE segment = 'SMB') AS smb_rows,
        COUNT(DISTINCT segment) AS distinct_segments,
        RANK() OVER (PARTITION BY CASE WHEN next_credit_limit >= 15900 THEN 'HIGH' ELSE 'LOW' END ORDER BY AVG(next_age_adjusted) FILTER (WHERE country = 'UK') DESC) AS uk_avg_next_age_adjusted_rank
    FROM cleaned
    GROUP BY 1, 2
    HAVING ROUND(AVG(row_seq), 2) > 0
)
SELECT
    next_credit_limit_tier,
    next_age_adjusted_tier,
    uk_avg_next_age_adjusted,
    de_count,
    avg_row_seq,
    smb_rows,
    distinct_segments,
    uk_avg_next_age_adjusted_rank
FROM stats
ORDER BY distinct_segments DESC
