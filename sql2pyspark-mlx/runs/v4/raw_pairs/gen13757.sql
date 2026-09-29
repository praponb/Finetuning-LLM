WITH ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM payments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at DESC, id) = 1
)
-- Grouped metrics
SELECT
    method,
    MAX(amount) AS max_amount,
    SORT_ARRAY(COLLECT_SET(status)) AS status_list,
    STDDEV_SAMP(amount) AS amount_stddev
FROM ranked
GROUP BY 1;
