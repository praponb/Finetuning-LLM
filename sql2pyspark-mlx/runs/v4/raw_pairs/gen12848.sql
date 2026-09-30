WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        CEIL(amount) AS amount_whole,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM orders
    WHERE order_date > DATE '2023-04-21'
      AND store_id IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        amount_whole,
        channel_clean,
        RANK() OVER (PARTITION BY store_id ORDER BY amount_whole DESC) AS amount_whole_rank,
        ROW_NUMBER() OVER (ORDER BY amount_whole DESC, id) AS event_seq
    FROM filtered
),
joined AS (
    -- Attach stores attributes
    SELECT
        s.*,
        p2.opened_date,
        p2.region
    FROM with_windows s
    LEFT JOIN stores p2
        ON s.store_id = p2.id
)
-- Aggregate with conditional metrics
SELECT
    store_id,
    PERCENTILE_APPROX(amount_whole_rank, ARRAY(0.25, 0.5, 0.75)) AS amount_whole_rank_quartiles,
    COUNT(CASE WHEN region = 'Central' THEN 1 END) AS central_count,
    COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count,
    SUM(CASE WHEN status = 'PENDING' THEN event_seq ELSE 0 END) AS pending_event_seq,
    ROUND(SUM(amount_whole) / NULLIF(COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END), 0), 2) AS amount_whole_per_completed
FROM joined
GROUP BY store_id
