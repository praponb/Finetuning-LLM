WITH with_dims AS (
    -- Attach customers attributes
    SELECT
        src.id,
        src.customer_id,
        src.browser,
        p.credit_limit,
        p.signup_date,
        p.age
    FROM sessions src
    LEFT JOIN customers p
        ON src.customer_id = p.id
    WHERE p.country = 'TH'
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        browser,
        signup_date,
        AVG(credit_limit) OVER w_ordered AS credit_limit_avg_2,
        AVG(age) OVER (PARTITION BY customer_id, browser ORDER BY credit_limit, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS age_avg_5,
        CUME_DIST() OVER (PARTITION BY browser ORDER BY credit_limit DESC) AS credit_limit_cume_dist,
        NTILE(3) OVER (PARTITION BY customer_id ORDER BY credit_limit, id) AS credit_limit_bucket,
        SUM(age) OVER w_running AS cumulative_age
    FROM with_dims
    WINDOW w_ordered AS (PARTITION BY browser ORDER BY credit_limit, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_running AS (PARTITION BY browser ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_dims_sessions AS (
    -- Attach customers attributes
    SELECT
        s.*,
        d.credit_limit,
        d.name,
        d.segment
    FROM enriched s
    INNER JOIN customers d
        ON s.customer_id = d.id
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY browser ORDER BY signup_date DESC, id) AS row_num
    FROM with_dims_sessions
    QUALIFY row_num = 1
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.signup_date,
        e.age_avg_5,
        e.credit_limit_cume_dist,
        e.credit_limit_bucket,
        e.credit_limit,
        e.row_num,
        c.country,
        c.age
    FROM deduped e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
)
SELECT
    id,
    customer_id,
    age_avg_5,
    credit_limit_cume_dist,
    credit_limit_bucket,
    row_num,
    age
FROM joined
ORDER BY row_num NULLS LAST;
