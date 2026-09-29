SELECT
    *,
    AVG(credit_limit) OVER w_hist AS credit_limit_avg_2_prior,
    CUME_DIST() OVER w_running AS credit_limit_cume_dist,
    CORR(age, credit_limit) OVER (PARTITION BY segment) AS corr_age_credit_limit,
    LAG(age, 1) OVER (ORDER BY age, id) AS prev_age,
    NTILE(4) OVER w_part AS credit_limit_bucket
FROM customers
WINDOW w_hist AS (PARTITION BY segment ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING), w_running AS (PARTITION BY segment, city ORDER BY credit_limit DESC NULLS FIRST), w_part AS (PARTITION BY country ORDER BY credit_limit, id)
