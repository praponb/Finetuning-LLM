WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(status, ', ', channel) AS status_channel,
        CONCAT_WS('/', status, channel) AS status_channel_key
    FROM orders
    WHERE YEAR(order_date) = 2025
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        discount,
        status_channel
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY discount NULLS LAST, id) AS row_num
        FROM base
    ) t
    WHERE row_num = 1
),
latest_match AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.paid_at
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.paid_at
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.amount DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM payments ch
            WHERE ch.order_id = s.id
              AND ch.paid_at < s.order_date
        )
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct
    FROM eligible a
    JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.order_date >= pr.starts_on
       AND a.order_date < pr.ends_on + INTERVAL '7' DAY
)
SELECT
    id,
    order_date,
    status,
    channel,
    discount,
    status_channel,
    paid_at,
    promo_id,
    promo_code,
    CASE WHEN status RLIKE '^(CANCELLED|PENDING)$' THEN 'CORE' WHEN status RLIKE '^(COMPLETED|SHIPPED)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS status_zone
FROM matched
ORDER BY promo_id DESC NULLS LAST;
