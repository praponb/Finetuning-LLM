WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        a.id,
        a.customer_id,
        a.start_date,
        c.email,
        c.credit_limit,
        c.city
    FROM subscriptions a
    INNER JOIN customers c
        ON a.customer_id <=> c.id
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(credit_limit) OVER w_hist AS last4_min_credit_limit,
        AVG(credit_limit) OVER w_hist AS last4_avg_credit_limit,
        COUNT(credit_limit) OVER w_hist AS last4_count_credit_limit,
        FIRST_VALUE(credit_limit, TRUE) OVER (PARTITION BY city ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_credit_limit
    FROM with_dims
    WINDOW w_hist AS (PARTITION BY city ORDER BY start_date, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        start_date,
        email,
        credit_limit,
        last4_avg_credit_limit,
        LEAD(first_known_credit_limit) OVER (PARTITION BY customer_id ORDER BY first_known_credit_limit, id) AS next_first_known_credit_limit,
        LEAD(last4_min_credit_limit) OVER (PARTITION BY customer_id ORDER BY first_known_credit_limit, id) AS next_last4_min_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY first_known_credit_limit, id) AS seq_no
    FROM metrics
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (PARTITION BY customer_id ORDER BY seq_no, id) AS seq_no_bucket,
        ROUND(last4_avg_credit_limit / SUM(last4_avg_credit_limit) OVER (PARTITION BY customer_id), 4) AS last4_avg_credit_limit_share,
        FIRST_VALUE(next_last4_min_credit_limit) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_next_last4_min_credit_limit
    FROM scored
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        p.signup_date,
        p.country,
        p.age
    FROM enriched e
    INNER JOIN customers p
        ON e.customer_id = p.id
)
SELECT
    id,
    customer_id,
    email,
    seq_no,
    seq_no_bucket,
    first_known_next_last4_min_credit_limit,
    signup_date
FROM joined
ORDER BY id NULLS LAST;
