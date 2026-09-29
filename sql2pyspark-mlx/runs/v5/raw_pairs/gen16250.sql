WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.discount > 0.16
        )
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY credit_limit) AS rank_in_group
    FROM kept
    QUALIFY rank_in_group = 1
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        credit_limit,
        age
    FROM ranked
    QUALIFY ROW_NUMBER() OVER (PARTITION BY city ORDER BY signup_date DESC, id) = 1
),
ranked_customers AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment, city ORDER BY credit_limit NULLS FIRST, id) = 1
),
daily AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN credit_limit >= 12300 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_tier,
        DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
        MAX(credit_limit) FILTER (WHERE country = 'DE') AS de_max_credit_limit,
        SUM(CASE WHEN segment = 'SMB' THEN credit_limit ELSE 0 END) AS smb_credit_limit,
        COUNT(*) FILTER (WHERE city = 'Berlin') AS berlin_rows,
        ROUND(SUM(credit_limit) / NULLIF(COUNT(CASE WHEN segment = 'SMB' THEN 1 END), 0), 2) AS credit_limit_per_smb,
        PERCENTILE_APPROX(age, 0.95) AS age_p95,
        AVG(age) AS avg_age
    FROM ranked_customers
    GROUP BY GROUPING SETS ((CASE WHEN credit_limit >= 12300 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', signup_date)), (CASE WHEN credit_limit >= 12300 THEN 'LARGE' ELSE 'SMALL' END))
    HAVING MAX(credit_limit) FILTER (WHERE country = 'DE') < 1000000
)
SELECT
    credit_limit_tier,
    quarter_start,
    de_max_credit_limit,
    smb_credit_limit,
    berlin_rows,
    age_p95,
    avg_age
FROM daily
