WITH scored AS (
    SELECT
        id,
        name,
        segment,
        signup_date,
        age,
        LAG(credit_limit) OVER w_hist AS prev_credit_limit,
        LAG(age) OVER w_hist AS prev_age,
        ROW_NUMBER() OVER w_hist AS row_seq
    FROM customers
    WHERE LOWER(TRIM(name)) = 'david lee'
    WINDOW w_hist AS (PARTITION BY segment, city ORDER BY signup_date, id)
),
metrics AS (
    SELECT
        *,
        LAST_VALUE(segment) IGNORE NULLS OVER (PARTITION BY segment ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_segment,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY prev_credit_limit NULLS FIRST) AS prev_credit_limit_dense_rank,
        ROUND(age / SUM(age) OVER (PARTITION BY segment), 4) AS age_share,
        ROW_NUMBER() OVER w_hist AS row_seq_2
    FROM scored
    WINDOW w_hist AS (PARTITION BY segment ORDER BY signup_date DESC, id)
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY segment ORDER BY row_seq_2 DESC NULLS LAST) AS row_seq_2_dense_rank,
        RANK() OVER (PARTITION BY segment ORDER BY row_seq_2) AS row_seq_2_rank
    FROM metrics
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY segment ORDER BY prev_credit_limit_dense_rank DESC, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
metrics_customers AS (
    SELECT
        id,
        age,
        prev_credit_limit,
        prev_credit_limit_dense_rank,
        age_share,
        row_seq_2_rank,
        rank_in_group,
        NTILE(5) OVER (PARTITION BY segment ORDER BY row_seq_2 DESC, id) AS row_seq_2_bucket,
        AVG(row_seq) OVER (PARTITION BY segment ORDER BY prev_age, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS row_seq_avg_6_prior,
        FIRST_VALUE(segment) IGNORE NULLS OVER (PARTITION BY segment ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_segment,
        AVG(row_seq_2) OVER (PARTITION BY segment ORDER BY rank_in_group, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) AS row_seq_2_avg_2_prior
    FROM latest
)
SELECT
    id,
    age,
    prev_credit_limit,
    prev_credit_limit_dense_rank,
    age_share,
    row_seq_2_rank,
    row_seq_2_bucket,
    row_seq_avg_6_prior,
    first_known_segment
FROM metrics_customers;
