WITH valid AS (
    SELECT
        id,
        name,
        city,
        segment,
        is_active,
        credit_limit,
        age,
        LAST_DAY(signup_date) AS signup_date_month_end
    FROM customers
    WHERE signup_date > DATE '2024-01-08'
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY signup_date_month_end DESC, id) AS dedup_rank
        FROM valid
    ) t
    WHERE dedup_rank = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY city ORDER BY signup_date_month_end DESC NULLS LAST) AS rank_in_group
        FROM deduped
    ) t
    WHERE rank_in_group = 1
),
valid_customers AS (
    SELECT
        id,
        city,
        segment,
        credit_limit,
        age,
        signup_date_month_end,
        dedup_rank,
        CONCAT_WS('/', name, city) AS name_city_key
    FROM ranked
    WHERE segment IS NOT NULL
),
kept AS (
    SELECT s.*
    FROM valid_customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at <= s.signup_date_month_end
        )
),
scored AS (
    SELECT
        *,
        LEAD(age, 1) OVER (PARTITION BY city, segment ORDER BY signup_date_month_end, id) AS next_age,
        NTILE(4) OVER (PARTITION BY city, segment ORDER BY credit_limit DESC, id) AS credit_limit_bucket,
        RANK() OVER (PARTITION BY city ORDER BY credit_limit DESC) AS credit_limit_rank
    FROM kept
),
stats AS (
    SELECT
        DATE_TRUNC('QUARTER', signup_date_month_end) AS quarter_start,
        CASE WHEN credit_limit >= 15000 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
        SUM(next_age) FILTER (WHERE city = 'London') AS london_total_next_age,
        CORR(credit_limit, age) AS corr_credit_limit_age,
        COUNT(CASE WHEN segment = 'SMB' THEN 1 END) AS smb_count,
        COUNT(*) FILTER (WHERE segment = 'Consumer') AS consumer_rows,
        PERCENTILE_APPROX(next_age, 0.5) AS next_age_p50,
        MAX(credit_limit_rank) FILTER (WHERE segment = 'Consumer') AS consumer_max_credit_limit_rank
    FROM scored
    GROUP BY DATE_TRUNC('QUARTER', signup_date_month_end), CASE WHEN credit_limit >= 15000 THEN 'PREMIUM' ELSE 'STANDARD' END
    HAVING consumer_rows > 1
)
SELECT
    quarter_start,
    credit_limit_tier,
    corr_credit_limit_age,
    next_age_p50
FROM stats
ORDER BY credit_limit_tier DESC;
