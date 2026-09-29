WITH valid AS (
    SELECT
        id,
        customer_id,
        end_date,
        monthly_fee,
        status,
        SUBSTRING(plan, 1, 3) AS plan_prefix,
        CAST(monthly_fee AS INT) AS monthly_fee_int
    FROM subscriptions
    WHERE start_date > DATE '2023-03-29'
      AND customer_id IS NOT NULL
),
deduped AS (
    SELECT *
    FROM valid
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY end_date DESC) = 1
),
kept_keys AS (
    SELECT status, customer_id
    FROM deduped
    EXCEPT
    SELECT status, customer_id
    FROM subscriptions
    WHERE MONTH(end_date) IN (3, 7, 12)
),
kept_rows AS (
    SELECT s.*
    FROM deduped s
    INNER JOIN kept_keys k ON s.status = k.status AND s.customer_id = k.customer_id
),
valid_subscriptions AS (
    SELECT
        id,
        customer_id,
        end_date,
        monthly_fee,
        status,
        plan_prefix,
        monthly_fee_int,
        LAST_DAY(end_date) AS end_date_month_end
    FROM kept_rows
    WHERE end_date > DATE '2023-12-05'
      AND customer_id IS NOT NULL
),
joined AS (
    SELECT
        s.*,
        b.country,
        b.name
    FROM valid_subscriptions s
    INNER JOIN customers b
        ON s.customer_id = b.id
)
SELECT
    s.id,
    s.customer_id,
    s.end_date,
    s.monthly_fee,
    s.plan_prefix,
    s.country,
    s.name,
    (SELECT MAX(o2.shipping_fee) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS max_order_shipping_fee
FROM joined s
ORDER BY country DESC;
