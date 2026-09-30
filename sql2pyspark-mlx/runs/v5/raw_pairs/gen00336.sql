WITH valid AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        status,
        CEIL(cost) cost_whole,
        COALESCE(carrier, 'DIRECT') carrier_clean
    FROM shipments
    WHERE shipped_date >= DATE '2023-06-14'
      AND shipped_date BETWEEN '2023-06-19' AND '2025-03-21'
)
SELECT
    id,
    cost_whole,
    DENSE_RANK() OVER (PARTITION BY status, carrier ORDER BY cost) cost_dense_rank,
    ROW_NUMBER() OVER w_running row_seq,
    NTILE(3) OVER (PARTITION BY carrier, status ORDER BY cost, id) cost_bucket
FROM valid
WINDOW w_running AS (ORDER BY cost_whole, id)
