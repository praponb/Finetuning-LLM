WITH cleaned AS (
    SELECT
        id,
        carrier,
        delivered_date,
        cost,
        status,
        TRIM(carrier) AS clean_carrier,
        COALESCE(carrier, 'OTHER') AS carrier_clean
    FROM shipments
    WHERE MONTH(shipped_date) IN (2, 6, 9)
),
with_windows AS (
    SELECT
        *,
        AVG(cost) OVER (ORDER BY cost, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS cost_avg_6,
        MAX(cost) OVER (PARTITION BY status) AS status_max_cost
    FROM cleaned
    WHERE status = 'IN_TRANSIT'
),
scored AS (
    SELECT
        id,
        carrier,
        delivered_date,
        clean_carrier,
        cost_avg_6,
        LAG(cost_avg_6) OVER w_recent AS prev_cost_avg_6,
        LEAD(cost_avg_6) OVER w_recent AS next_cost_avg_6,
        LAG(cost) OVER w_recent AS prev_cost,
        LEAD(cost) OVER w_recent AS next_cost,
        ROW_NUMBER() OVER w_recent AS event_seq
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY carrier ORDER BY cost, id)
),
cleaned_shipments AS (
    SELECT
        id,
        delivered_date,
        clean_carrier,
        prev_cost_avg_6,
        prev_cost,
        event_seq,
        SUBSTRING(carrier, 1, 2) AS carrier_prefix,
        COALESCE(carrier, 'OTHER') AS carrier_filled
    FROM scored
    WHERE delivered_date >= DATE '2023-09-06'
),
scored_shipments AS (
    SELECT
        *,
        FIRST_VALUE(prev_cost, TRUE) OVER (ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_prev_cost,
        DENSE_RANK() OVER (ORDER BY prev_cost_avg_6 NULLS LAST) AS prev_cost_avg_6_dense_rank
    FROM cleaned_shipments
),
remaining_keys AS (
    SELECT id
    FROM scored_shipments
    EXCEPT
    SELECT id
    FROM shipments
    WHERE delivered_date BETWEEN '2023-03-01' AND '2025-09-01'
),
reconciled AS (
    SELECT s.*
    FROM scored_shipments s
    INNER JOIN remaining_keys k ON s.id = k.id
),
with_windows_shipments AS (
    SELECT
        prev_cost,
        event_seq,
        carrier_prefix,
        DENSE_RANK() OVER (ORDER BY prev_cost_avg_6 DESC NULLS FIRST) AS prev_cost_avg_6_dense_rank_2,
        DENSE_RANK() OVER (ORDER BY prev_cost_avg_6_dense_rank NULLS LAST) AS prev_cost_avg_6_dense_rank_dense_rank
    FROM reconciled
)
SELECT
    prev_cost,
    event_seq,
    carrier_prefix,
    prev_cost_avg_6_dense_rank_2
FROM with_windows_shipments
ORDER BY event_seq DESC NULLS FIRST
