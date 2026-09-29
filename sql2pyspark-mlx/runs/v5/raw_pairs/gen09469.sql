WITH with_dims AS (
    SELECT
        src.id,
        src.customer_id,
        src.plan,
        src.start_date,
        src.monthly_fee,
        src.status,
        p.age
    FROM subscriptions src
    LEFT JOIN customers p
        ON src.customer_id = p.id
),
enriched AS (
    SELECT
        a.id,
        a.customer_id,
        a.plan,
        a.start_date,
        a.monthly_fee,
        a.status,
        p.signup_date,
        p.email,
        p.segment
    FROM with_dims a
    JOIN customers p
        ON a.customer_id = p.id
),
enriched_subscriptions AS (
    SELECT
        id,
        customer_id,
        status,
        email,
        RANK() OVER (PARTITION BY segment ORDER BY monthly_fee DESC NULLS LAST) monthly_fee_rank,
        AVG(monthly_fee) OVER (PARTITION BY segment ORDER BY start_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) monthly_fee_avg_5_prior
    FROM enriched
),
filtered AS (
    SELECT
        id,
        customer_id,
        status,
        email,
        monthly_fee_rank,
        monthly_fee_avg_5_prior,
        CONCAT_WS('-', status, email) status_email_key,
        CEIL(monthly_fee_avg_5_prior) monthly_fee_avg_5_prior_whole
    FROM enriched_subscriptions
    WHERE monthly_fee_avg_5_prior < 200
),
cleaned AS (
    SELECT
        id,
        customer_id,
        status,
        email,
        monthly_fee_avg_5_prior_whole,
        UPPER(status) status_upper
    FROM filtered
    WHERE customer_id IS NOT NULL
),
scored AS (
    SELECT
        *,
        NTILE(10) OVER (ORDER BY monthly_fee_avg_5_prior_whole ASC, id ASC) monthly_fee_avg_5_prior_whole_bucket,
        NTILE(4) OVER w_part monthly_fee_avg_5_prior_whole_bucket_2,
        ROW_NUMBER() OVER (PARTITION BY status, customer_id ORDER BY monthly_fee_avg_5_prior_whole ASC, id ASC) row_seq,
        DENSE_RANK() OVER w_ordered monthly_fee_avg_5_prior_whole_dense_rank,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY monthly_fee_avg_5_prior_whole ASC, id ASC) event_seq
    FROM cleaned
    WINDOW w_part AS (PARTITION BY status ORDER BY monthly_fee_avg_5_prior_whole ASC, id ASC), w_ordered AS (PARTITION BY customer_id ORDER BY monthly_fee_avg_5_prior_whole DESC NULLS LAST)
)
SELECT
    CASE WHEN monthly_fee_avg_5_prior_whole >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END AS monthly_fee_avg_5_prior_whole_tier,
    CASE WHEN event_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    SIZE(COLLECT_SET(status)) AS num_status,
    COUNT(*) AS row_count
FROM scored
GROUP BY CASE WHEN monthly_fee_avg_5_prior_whole >= 89 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN event_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END;
