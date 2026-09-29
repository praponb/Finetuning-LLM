WITH with_windows AS (
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
        SUM(age) OVER (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_age,
        AVG(age) OVER (PARTITION BY country ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) age_avg_2_prior
    FROM customers
),
metrics AS (
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
        running_age,
        age_avg_2_prior,
        FIRST_VALUE(city, TRUE) OVER (ORDER BY running_age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_city,
        PERCENT_RANK() OVER w_part age_avg_2_prior_percent_rank
    FROM with_windows
    WINDOW w_part AS (PARTITION BY segment ORDER BY age_avg_2_prior)
),
enriched AS (
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
        running_age,
        age_avg_2_prior,
        first_known_city,
        age_avg_2_prior_percent_rank,
        LEAD(age_avg_2_prior, 1) OVER (PARTITION BY city ORDER BY age_avg_2_prior_percent_rank, id) next_age_avg_2_prior,
        NTILE(5) OVER (PARTITION BY country, city ORDER BY age_avg_2_prior DESC, id) age_avg_2_prior_bucket,
        STDDEV_SAMP(credit_limit) OVER (PARTITION BY country) country_stddev_samp_credit_limit,
        AVG(age_avg_2_prior_percent_rank) OVER (ORDER BY signup_date, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) age_avg_2_prior_percent_rank_avg_5,
        ROW_NUMBER() OVER (PARTITION BY country, segment ORDER BY age_avg_2_prior_percent_rank DESC, id) event_seq
    FROM metrics
),
filtered AS (
    SELECT *, INITCAP(TRIM(city)) city_title
    FROM enriched
    WHERE signup_date > DATE '2023-11-30'
),
cleaned AS (
    SELECT
        *,
        country_stddev_samp_credit_limit + age country_stddev_samp_credit_limit_plus_age,
        COALESCE(age_avg_2_prior_percent_rank_avg_5, 0) age_avg_2_prior_percent_rank_avg_5_filled
    FROM filtered
    WHERE next_age_avg_2_prior = 36
),
with_latest AS (
    SELECT
        s.*,
        latest.account_type,
        latest.account_status
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.account_type, c.status AS account_status
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM orders ch
            WHERE ch.customer_id = s.id
              AND ch.order_date < s.signup_date
        )
)
SELECT
    id,
    name,
    city,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    age_avg_2_prior
FROM kept
ORDER BY segment DESC, credit_limit DESC
