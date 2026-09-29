WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        LAG(cost) OVER (PARTITION BY status, order_id ORDER BY shipped_date, id) AS prev_cost,
        LEAD(cost) OVER (PARTITION BY status, order_id ORDER BY shipped_date, id) AS next_cost,
        ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY shipped_date, id) AS event_seq
    FROM shipments
),
kept_keys AS (
    -- Set difference on keys
    SELECT carrier, id
    FROM metrics
    EXCEPT ALL
    SELECT carrier, id
    FROM shipments
    WHERE status IN ('IN_TRANSIT', 'RETURNED', 'LOST')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    INNER JOIN kept_keys k ON s.carrier = k.carrier AND s.id = k.id
),
kept_keys_2 AS (
    -- Set difference on keys
    SELECT carrier, id
    FROM kept_rows
    EXCEPT
    SELECT carrier, id
    FROM shipments
    WHERE YEAR(shipped_date) >= 2023
),
kept_rows_shipments AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept_rows s
    INNER JOIN kept_keys_2 k ON s.carrier = k.carrier AND s.id = k.id
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        next_cost,
        event_seq,
        cost * event_seq AS cost_times_event_seq,
        GREATEST(prev_cost, event_seq) AS greatest_prev_cost_event_seq
    FROM kept_rows_shipments
    WHERE order_id IS NOT NULL
      AND shipped_date > TO_DATE('2023-07-05')
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.shipped_date,
    s.delivered_date,
    s.cost,
    s.status,
    s.event_seq,
    s.greatest_prev_cost_event_seq,
    (SELECT MIN(p2.amount) FROM payments p2 WHERE p2.order_id = s.order_id) AS min_payment_amount
FROM cleaned s
ORDER BY greatest_prev_cost_event_seq DESC NULLS LAST
