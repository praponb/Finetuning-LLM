WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ABS(balance) AS abs_balance,
        COALESCE(status, 'OTHER') AS status_filled
    FROM accounts
    WHERE customer_id IS NOT NULL
      AND opened_date >= DATE '2023-04-18'
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.*,
        c.name,
        c.credit_limit
    FROM cleaned e
    INNER JOIN customers c
        ON e.customer_id = c.id
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        a.*,
        p.signup_date,
        p.email,
        p.name AS customer_name
    FROM joined a
    LEFT JOIN customers p
        ON a.customer_id <=> p.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        customer_id,
        opened_date,
        abs_balance,
        status_filled,
        credit_limit,
        NTILE(4) OVER (ORDER BY balance, id) AS balance_bucket,
        NTILE(4) OVER (PARTITION BY customer_id ORDER BY abs_balance, id) AS abs_balance_bucket,
        PERCENT_RANK() OVER (PARTITION BY account_type ORDER BY credit_limit) AS credit_limit_percent_rank,
        NTILE(10) OVER (PARTITION BY status ORDER BY abs_balance, id) AS abs_balance_bucket_2,
        LEAD(abs_balance, 2, 0) OVER (PARTITION BY status ORDER BY abs_balance, id) AS next_abs_balance
    FROM with_dims
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY abs_balance DESC, id) AS latest_rank
    FROM metrics
    QUALIFY latest_rank = 1
)
SELECT
    id,
    opened_date,
    balance_bucket,
    abs_balance_bucket,
    credit_limit_percent_rank,
    latest_rank
FROM deduped
