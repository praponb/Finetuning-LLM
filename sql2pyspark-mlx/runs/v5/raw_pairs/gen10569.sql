WITH joined AS (
    -- Attach orders attributes
    SELECT
        e.id,
        e.order_id,
        e.method,
        e.paid_at,
        o.amount AS order_amount,
        o.channel
    FROM payments e
    INNER JOIN orders o
        ON e.order_id = o.id
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        paid_at,
        channel,
        COALESCE(method, 'Unknown') AS method_filled,
        COALESCE(method, 'OTHER') AS method_clean
    FROM joined
    WHERE paid_at >= TO_TIMESTAMP('2023-11-12 04:00:00')
      AND order_id IS NOT NULL
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        channel,
        method_filled,
        method_clean
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY paid_at DESC, id) AS row_num
        FROM filtered
    ) x
    WHERE row_num = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(channel) AS channel_upper
    FROM deduped
    WHERE channel IS NOT NULL
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN channel IN ('web', 'mobile') THEN 'Priority' ELSE 'Other' END AS channel_group,
        COALESCE(channel, 'OTHER') AS channel_clean
    FROM cleaned
    WHERE channel = 'store'
),
daily AS (
    -- Grouped metrics
    SELECT
        channel,
        SORT_ARRAY(COLLECT_SET(channel)) AS channel_list,
        COUNT(CASE WHEN channel = 'mobile' THEN 1 END) AS mobile_count
    FROM base
    GROUP BY channel
)
SELECT channel, channel_list
FROM daily
ORDER BY channel DESC
LIMIT 10;
