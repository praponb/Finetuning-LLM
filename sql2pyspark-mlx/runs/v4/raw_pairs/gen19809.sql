WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at NULLS FIRST, id) AS row_num
    FROM sessions
    QUALIFY row_num = 1
),
attributed AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM ranked s
    LEFT JOIN subscriptions sub
        ON s.customer_id = sub.customer_id
       AND s.started_at >= sub.start_date
       AND s.started_at < sub.end_date + INTERVAL 14 DAYS
),
matched_keys AS (
    -- Set difference on keys
    SELECT browser, country
    FROM attributed
    EXCEPT
    SELECT browser, country
    FROM sessions
    WHERE browser != 'Safari'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM attributed s
    INNER JOIN matched_keys k ON s.browser = k.browser AND s.country = k.country
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CONCAT(country, ' ', plan) AS country_plan
    FROM kept_rows
    WHERE started_at > TIMESTAMP '2023-05-07 11:00:00'
      AND device NOT IN ('desktop', 'mobile')
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.browser,
    s.started_at,
    s.country,
    s.row_num,
    s.plan,
    s.country_plan,
    (SELECT MIN(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS min_account_balance
FROM filtered s;
