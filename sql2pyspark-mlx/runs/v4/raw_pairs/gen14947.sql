WITH kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM customers s
    WHERE NOT EXISTS (
            SELECT 1
            FROM sessions x
            WHERE x.customer_id = s.id
              AND x.started_at <= s.signup_date
        )
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM kept
    QUALIFY RANK() OVER (PARTITION BY segment, country ORDER BY signup_date) = 1
),
enriched AS (
    -- Ranking and gap features
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
        CORR(age, credit_limit) OVER (PARTITION BY city) AS corr_age_credit_limit,
        SUM(credit_limit) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit,
        STDDEV_SAMP(age) OVER (PARTITION BY country) AS country_stddev_samp_age,
        PERCENT_RANK() OVER w_recent AS age_percent_rank,
        NTILE(10) OVER (PARTITION BY segment ORDER BY age DESC, id) AS age_bucket
    FROM latest
    WINDOW w_recent AS (PARTITION BY city ORDER BY age DESC)
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM accounts ch
            WHERE ch.customer_id = s.id
              AND ch.opened_date <= s.signup_date
        )
)
SELECT
    id,
    email,
    segment,
    signup_date,
    is_active,
    age,
    corr_age_credit_limit,
    age_percent_rank,
    age_bucket
FROM eligible
ORDER BY age_percent_rank DESC NULLS FIRST;
