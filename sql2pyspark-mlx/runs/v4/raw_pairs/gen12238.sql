WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        account_type,
        balance,
        opened_date,
        ADD_MONTHS(opened_date, 6) AS opened_date_plus_months,
        CONCAT(account_type, ' ', status) AS account_type_status,
        COALESCE(status, 'N/A') AS resolved_status
    FROM accounts
    WHERE opened_date >= TO_DATE('2023-05-14')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(balance) OVER (PARTITION BY customer_id) AS customer_id_max_balance,
        MAX(balance) OVER (PARTITION BY customer_id) AS customer_id_max_balance_2
    FROM cleaned
),
joined AS (
    -- Attach customers attributes
    SELECT e.*, b.city
    FROM metrics e
    LEFT JOIN customers b
        ON e.customer_id = b.id
),
grouped AS (
    -- Grouped metrics
    SELECT
        city,
        APPROX_PERCENTILE(customer_id_max_balance, ARRAY(0.25, 0.5, 0.75)) AS customer_id_max_balance_quartiles,
        MAX(customer_id_max_balance_2) FILTER (WHERE account_type = 'LOAN') AS loan_max_customer_id_max_balance_2,
        APPROX_PERCENTILE(customer_id_max_balance_2, ARRAY(0.25, 0.5, 0.75)) AS customer_id_max_balance_2_quartiles,
        PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles,
        SIZE(COLLECT_SET(account_type)) AS num_account_types,
        COUNT_IF(city = 'Singapore') AS singapore_events
    FROM joined
    GROUP BY city
)
SELECT
    city,
    customer_id_max_balance_quartiles,
    loan_max_customer_id_max_balance_2,
    CASE WHEN city RLIKE '^(New York|Tokyo)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS city_zone
FROM grouped
