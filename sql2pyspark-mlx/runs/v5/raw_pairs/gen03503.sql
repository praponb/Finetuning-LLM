WITH enriched AS (
    -- Attach orders attributes
    SELECT
        a.*,
        o.channel,
        o.amount AS order_amount,
        o.order_date
    FROM payments a
    JOIN orders o
        ON a.order_id <=> o.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, method ORDER BY order_amount DESC, id) AS rank_in_group
    FROM enriched
    QUALIFY rank_in_group = 1
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM ranked
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        status,
        channel,
        order_amount,
        rank_in_group,
        SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(burst_id) OVER (PARTITION BY channel ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS burst_id_sum_7d,
        ROUND(amount / SUM(amount) OVER (PARTITION BY status), 4) AS amount_share
    FROM islands
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM scored e
    JOIN promotions pr
        ON e.channel = pr.channel
       AND e.paid_at >= pr.starts_on
       AND e.paid_at < pr.ends_on + INTERVAL '7' DAY
)
SELECT
    s.id,
    s.order_id,
    s.amount,
    s.paid_at,
    s.status,
    s.channel,
    s.rank_in_group,
    s.burst_id,
    s.burst_id_sum_7d,
    CASE WHEN channel RLIKE '^(mobile|partner)$' THEN 'GROUP_B' ELSE 'EXTENDED' END AS channel_zone,
    (SELECT MAX(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS max_shipment_cost
FROM matched s;
