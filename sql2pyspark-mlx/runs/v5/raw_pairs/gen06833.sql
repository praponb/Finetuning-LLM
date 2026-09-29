WITH kept_keys AS (
    SELECT id, plan
    FROM subscriptions
    EXCEPT ALL
    SELECT id, plan
    FROM subscriptions
    WHERE plan = 'premium'
),
reconciled AS (
    SELECT s.*
    FROM subscriptions s
    JOIN kept_keys k ON s.id = k.id AND s.plan = k.plan
),
filtered AS (
    SELECT
        id,
        customer_id,
        plan,
        status,
        DATE_SUB(start_date, 87) start_date_minus_87d
    FROM reconciled
    WHERE end_date > DATE '2023-11-06'
      AND customer_id IS NOT NULL
),
joined AS (
    SELECT
        s.*,
        d.age,
        d.credit_limit,
        d.segment
    FROM filtered s
    LEFT JOIN customers d
        ON s.customer_id = d.id
),
with_dims AS (
    SELECT
        a.customer_id,
        a.start_date_minus_87d,
        a.credit_limit,
        a.segment,
        b.credit_limit AS customer_credit_limit,
        b.email
    FROM joined a
    JOIN customers b
        ON a.customer_id <=> b.id
    WHERE b.email LIKE '%@startup.io'
),
enriched AS (
    SELECT
        src.customer_id,
        src.start_date_minus_87d,
        src.credit_limit,
        src.email,
        b.city,
        b.email AS customer_email,
        b.segment AS customer_segment
    FROM with_dims src
    LEFT JOIN customers b
        ON src.customer_id <=> b.id
),
with_dims_subscriptions AS (
    SELECT
        e.*,
        p.credit_limit AS customer_credit_limit,
        p.email AS customer_email_2,
        p.segment
    FROM enriched e
    JOIN customers p
        ON e.customer_id = p.id
),
with_windows AS (
    SELECT
        customer_id,
        credit_limit,
        city,
        customer_email,
        customer_credit_limit,
        STDDEV_SAMP(credit_limit) OVER (PARTITION BY customer_id, city) customer_id_stddev_samp_credit_limit,
        CUME_DIST() OVER w_hist customer_credit_limit_cume_dist,
        MIN(customer_credit_limit) OVER (PARTITION BY customer_id) customer_id_min_customer_credit_limit,
        ROUND(credit_limit / SUM(credit_limit) OVER (PARTITION BY segment), 4) credit_limit_share,
        CUME_DIST() OVER (PARTITION BY city ORDER BY credit_limit) credit_limit_cume_dist
    FROM with_dims_subscriptions
    WINDOW w_hist AS (PARTITION BY city, customer_id ORDER BY customer_credit_limit NULLS LAST)
)
SELECT
    *,
    DENSE_RANK() OVER (ORDER BY customer_id_stddev_samp_credit_limit DESC NULLS LAST) customer_id_stddev_samp_credit_limit_dense_rank,
    AVG(customer_id_stddev_samp_credit_limit) OVER (PARTITION BY customer_id, city) customer_id_avg_customer_id_stddev_samp_credit_limit
FROM with_windows
