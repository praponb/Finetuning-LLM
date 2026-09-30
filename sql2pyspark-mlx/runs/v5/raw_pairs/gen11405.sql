WITH with_last AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.country,
        s.signup_date,
        s.is_active,
        s.credit_limit,
        s.age,
        top1.account_opened_date
    FROM customers s
    JOIN LATERAL (
            SELECT c.opened_date AS account_opened_date
            FROM accounts c
            WHERE c.customer_id = s.id
              AND c.opened_date <= s.signup_date
            ORDER BY c.opened_date DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(age) OVER w_ordered AS last3_avg_age,
        COUNT(age) OVER w_ordered AS last3_count_age,
        SUM(age) OVER w_ordered AS last3_sum_age,
        LEAD(age, 1) OVER (PARTITION BY country ORDER BY signup_date, id) AS next_age
    FROM with_last
    WINDOW w_ordered AS (PARTITION BY country ORDER BY age, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
enriched_customers AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(last3_sum_age) OVER (PARTITION BY country ORDER BY account_opened_date, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS last3_sum_age_avg_3_prior,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY account_opened_date, id) AS row_seq
    FROM enriched
),
with_latest AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.order_discount,
        x.status
    FROM enriched_customers s
    JOIN LATERAL (
            SELECT c.discount AS order_discount, c.status
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.discount DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest_customers AS (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.name,
        s.country,
        s.is_active,
        s.credit_limit,
        s.age,
        s.last3_count_age,
        s.row_seq,
        s.order_discount,
        s.status,
        latest.channel,
        latest.discount
    FROM with_latest s
    LEFT JOIN LATERAL (
            SELECT c.channel, c.discount
            FROM orders c
            WHERE c.customer_id = s.id
            ORDER BY c.order_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        country,
        credit_limit,
        order_discount,
        discount
    FROM with_latest_customers
) src
UNPIVOT (
    value FOR measure IN (credit_limit AS CREDIT_LIMIT, order_discount AS ORDER_DISCOUNT_VAL, discount)
);
