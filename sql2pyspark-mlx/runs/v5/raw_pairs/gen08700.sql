WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        REGEXP_REPLACE(email, ' ', '-') AS email_slug
    FROM customers
    WHERE country = 'JP'
      AND signup_date >= DATE '2023-05-09'
),
with_last AS (
    -- Latest matching subscription per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.end_date,
        x.start_date
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.end_date, c.start_date
            FROM subscriptions c
            WHERE c.customer_id = s.id
            ORDER BY c.end_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_last_customers AS (
    -- Latest matching account per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.email,
        s.segment,
        s.signup_date,
        s.credit_limit,
        s.email_slug,
        x.account_type,
        x.account_opened_date
    FROM with_last s
    JOIN LATERAL (
            SELECT c.account_type, c.opened_date AS account_opened_date
            FROM accounts c
            WHERE c.customer_id = s.id
            ORDER BY c.balance DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY segment ORDER BY credit_limit, id) AS row_seq,
        SUM(credit_limit) OVER (ORDER BY account_opened_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_credit_limit,
        LAG(credit_limit, 1) OVER (PARTITION BY segment ORDER BY account_opened_date, id) AS prev_credit_limit
    FROM with_last_customers
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        account_type,
        segment,
        row_seq
    FROM enriched
) src
PIVOT (
    MAX(row_seq) AS value_sum,
    COUNT(1) AS n
    FOR segment IN ('Consumer', 'Corporate')
);
