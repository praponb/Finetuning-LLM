WITH filtered AS (
    SELECT
        id,
        email,
        country,
        segment,
        signup_date,
        age,
        COALESCE(credit_limit, 0) AS credit_limit_filled
    FROM customers
    WHERE credit_limit >= 4700
      AND segment IS NOT NULL
),
deduped AS (
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, segment ORDER BY signup_date DESC NULLS FIRST, id) = 1
),
latest_match AS (
    SELECT
        s.id,
        s.email,
        s.signup_date,
        s.credit_limit_filled,
        x.amount,
        x.order_order_date
    FROM deduped s
    LEFT JOIN LATERAL (
            SELECT c.amount, c.order_date AS order_order_date
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
metrics AS (
    SELECT
        *,
        LAST_VALUE(amount, TRUE) OVER w_grp AS last_known_amount,
        DENSE_RANK() OVER (ORDER BY credit_limit_filled) AS credit_limit_filled_dense_rank,
        SUM(amount) OVER w_grp AS cumulative_amount
    FROM latest_match
    WHERE amount BETWEEN 550 AND 630
    WINDOW w_grp AS (ORDER BY signup_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT *
FROM (
    SELECT
        id,
        amount,
        credit_limit_filled,
        last_known_amount
    FROM metrics
) src
UNPIVOT (
    val FOR kpi IN (amount, credit_limit_filled, last_known_amount)
)
