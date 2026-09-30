WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY segment ORDER BY age DESC, id ASC) = 1
),
with_last AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.account_type,
        top1.opened_date
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.account_type, c.opened_date
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(credit_limit) OVER (PARTITION BY country, city ORDER BY signup_date ASC, id ASC ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS credit_limit_avg_5_prior,
        CORR(age, credit_limit) OVER w_ordered AS corr_age_credit_limit,
        DENSE_RANK() OVER w_recent AS credit_limit_dense_rank
    FROM with_last
    WINDOW w_ordered AS (PARTITION BY segment), w_recent AS (ORDER BY credit_limit ASC NULLS LAST)
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        ROW_NUMBER() OVER (ORDER BY credit_limit DESC, id ASC) AS event_seq,
        SUM(corr_age_credit_limit) OVER (PARTITION BY country ORDER BY credit_limit ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_corr_age_credit_limit,
        ROW_NUMBER() OVER (PARTITION BY account_type, city ORDER BY credit_limit_avg_5_prior ASC, id ASC) AS row_seq
    FROM with_windows
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN credit_limit_avg_5_prior >= 15433 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_avg_5_prior_tier,
    MAX(credit_limit_avg_5_prior) AS max_credit_limit_avg_5_prior,
    PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
    COUNT_IF(city = 'London') AS london_events,
    BIT_OR(age) AS age_bit_or
FROM scored
GROUP BY CASE WHEN credit_limit_avg_5_prior >= 15433 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING MAX(credit_limit_avg_5_prior) >= 0
ORDER BY credit_limit_avg_5_prior_tier DESC NULLS FIRST;
