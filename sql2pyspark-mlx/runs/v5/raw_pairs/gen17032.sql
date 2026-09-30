WITH metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY country ORDER BY credit_limit DESC NULLS FIRST) AS credit_limit_cume_dist,
        CORR(credit_limit, age) OVER (PARTITION BY country) AS corr_credit_limit_age,
        CORR(age, credit_limit) OVER (PARTITION BY country) AS corr_age_credit_limit
    FROM customers
),
filtered AS (
    SELECT
        *,
        corr_age_credit_limit - corr_credit_limit_age AS corr_age_credit_limit_minus_corr_credit_limit_age,
        COALESCE(city, 'NONE') AS resolved_city
    FROM metrics
    WHERE signup_date >= DATE '2023-05-18'
),
cleaned AS (
    SELECT
        *,
        credit_limit - age AS credit_limit_minus_age,
        UPPER(segment) AS segment_upper
    FROM filtered
    WHERE segment IS NOT NULL
      AND credit_limit_cume_dist BETWEEN 0.3 AND 0.74
),
enriched AS (
    SELECT
        id,
        name,
        city,
        segment,
        signup_date,
        is_active,
        age,
        corr_credit_limit_age,
        corr_age_credit_limit,
        resolved_city,
        credit_limit_minus_age,
        NTILE(10) OVER (ORDER BY corr_credit_limit_age DESC, id ASC) AS corr_credit_limit_age_bucket,
        RANK() OVER (PARTITION BY country ORDER BY corr_age_credit_limit DESC) AS corr_age_credit_limit_rank
    FROM cleaned
),
summary AS (
    SELECT
        DATE_TRUNC('YEAR', signup_date) AS year_start,
        CASE WHEN corr_age_credit_limit_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS corr_age_credit_limit_rank_tier,
        CASE WHEN credit_limit_minus_age >= 88 THEN 'LARGE' ELSE 'SMALL' END AS credit_limit_minus_age_tier,
        AVG(corr_credit_limit_age) FILTER (WHERE segment = 'SMB') AS smb_avg_corr_credit_limit_age,
        PERCENTILE_APPROX(corr_credit_limit_age, 0.95) AS corr_credit_limit_age_p95,
        COUNT(*) AS event_count
    FROM enriched
    GROUP BY CUBE (DATE_TRUNC('YEAR', signup_date), CASE WHEN corr_age_credit_limit_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN credit_limit_minus_age >= 88 THEN 'LARGE' ELSE 'SMALL' END)
    HAVING COUNT(*) > 1
)
SELECT
    year_start,
    credit_limit_minus_age_tier,
    smb_avg_corr_credit_limit_age,
    corr_credit_limit_age_p95
FROM summary
ORDER BY smb_avg_corr_credit_limit_age ASC, corr_credit_limit_age_p95 ASC
