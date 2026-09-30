WITH valid AS (
    SELECT
        *,
        INITCAP(TRIM(segment)) AS segment_title,
        COALESCE(segment, 'N/A') AS segment_filled
    FROM customers
    WHERE signup_date >= DATE '2023-09-04'
),
latest_match AS (
    SELECT
        s.*,
        top1.plan,
        top1.end_date
    FROM valid s
    LEFT JOIN LATERAL (
            SELECT c.plan, c.end_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
              AND c.end_date <= s.signup_date
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
scored AS (
    SELECT
        id,
        name,
        email,
        country,
        age,
        segment_title,
        segment_filled,
        ROW_NUMBER() OVER (ORDER BY credit_limit, id) AS seq_no,
        NTILE(10) OVER (PARTITION BY plan ORDER BY credit_limit, id) AS credit_limit_bucket,
        AVG(age) OVER (PARTITION BY city ORDER BY signup_date, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS age_avg_6_prior
    FROM latest_match
),
latest AS (
    SELECT *
    FROM scored
    QUALIFY RANK() OVER (PARTITION BY country ORDER BY credit_limit_bucket DESC NULLS FIRST) = 1
),
kept_keys AS (
    SELECT country, id
    FROM latest
    EXCEPT ALL
    SELECT country, id
    FROM customers
    WHERE LENGTH(name) >= 8
),
verified AS (
    SELECT s.*
    FROM latest s
    INNER JOIN kept_keys k ON s.country = k.country AND s.id = k.id
),
with_windows AS (
    SELECT
        name,
        email,
        country,
        segment_title,
        seq_no,
        credit_limit_bucket,
        age_avg_6_prior,
        PERCENT_RANK() OVER w_grp AS seq_no_percent_rank,
        MIN(seq_no) OVER w_hist AS country_min_seq_no,
        AVG(age) OVER w_hist AS country_avg_age
    FROM verified
    WINDOW w_grp AS (ORDER BY seq_no NULLS LAST), w_hist AS (PARTITION BY country)
),
cleaned AS (
    SELECT
        name,
        email,
        country,
        segment_title,
        age_avg_6_prior,
        seq_no_percent_rank,
        country_min_seq_no,
        LEAST(country_avg_age, age_avg_6_prior) AS least_country_avg_age_age_avg_6_prior,
        country_avg_age / NULLIF(seq_no_percent_rank, 0) AS country_avg_age_per_seq_no_percent_rank
    FROM with_windows
    WHERE seq_no_percent_rank > 0.2
)
SELECT
    CASE WHEN age_avg_6_prior >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_avg_6_prior_tier,
    country_min_seq_no,
    APPROX_PERCENTILE(least_country_avg_age_age_avg_6_prior, 0.5) AS least_country_avg_age_age_avg_6_prior_p50,
    COUNT(CASE WHEN country = 'SG' THEN 1 END) AS sg_count,
    GROUPING_ID() AS grouping_level
FROM cleaned
GROUP BY GROUPING SETS ((CASE WHEN age_avg_6_prior >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END, country_min_seq_no), (CASE WHEN age_avg_6_prior >= 61 THEN 'PREMIUM' ELSE 'STANDARD' END));
