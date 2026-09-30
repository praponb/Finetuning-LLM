WITH base AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN segment IN ('Consumer', 'SMB') THEN 'Priority' ELSE 'Other' END AS segment_group
    FROM customers
    WHERE city IS NOT NULL
      AND signup_date > '2024-10-01'
),
with_last AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.email,
        s.country,
        s.is_active,
        s.age,
        s.segment_group,
        top1.amount
    FROM base s
    JOIN LATERAL (
            SELECT c.amount
            FROM orders c
            WHERE c.customer_id = s.id
              AND c.order_date <= s.signup_date
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY amount DESC) AS rank_in_group
    FROM with_last
    QUALIFY rank_in_group = 1
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM orders x
            WHERE x.customer_id = s.id
              AND x.order_date <= '2023-06-01'
        )
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        name,
        age,
        segment_group,
        AVG(amount) OVER (PARTITION BY country ORDER BY amount, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior,
        FIRST_VALUE(amount, TRUE) OVER (ORDER BY age, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM kept
    WHERE age BETWEEN 47 AND 50
),
scored_customers AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER w_hist AS row_seq,
        FIRST_VALUE(amount_avg_6_prior, TRUE) OVER (ORDER BY amount_avg_6_prior, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount_avg_6_prior,
        NTILE(4) OVER w_hist AS first_known_amount_bucket,
        AVG(amount_avg_6_prior) OVER (ORDER BY age, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS amount_avg_6_prior_avg_5_prior
    FROM scored
    WINDOW w_hist AS (ORDER BY first_known_amount DESC, id)
),
base_customers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(row_seq / amount_avg_6_prior_avg_5_prior, 2) AS row_seq_per_amount_avg_6_prior_avg_5_prior
    FROM scored_customers
    WHERE first_known_amount >= 430
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN first_known_amount_bucket >= 9 THEN 'HIGH' ELSE 'LOW' END AS first_known_amount_bucket_tier,
    BIT_AND(age) AS age_bit_and,
    PERCENTILE_APPROX(age, ARRAY(0.25, 0.5, 0.75)) AS age_quartiles,
    COUNT(DISTINCT id) AS distinct_ids
FROM base_customers
GROUP BY first_known_amount_bucket_tier;
