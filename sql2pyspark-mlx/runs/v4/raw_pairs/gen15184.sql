WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(status) AS clean_status,
        COALESCE(method, 'DIRECT') AS method_filled
    FROM payments
    WHERE paid_at >= TIMESTAMP '2024-03-12 16:00:00'
      AND method IS NOT NULL
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY order_id, method ORDER BY paid_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
        COUNT(amount) OVER (ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) AS amount_count_7d
    FROM valid
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount_count_7d, id) AS row_num
        FROM scored
    ) x
    WHERE row_num = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        method,
        order_id,
        status,
        amount
    FROM deduped
) src
PIVOT (
    SUM(amount) AS value_sum,
    COUNT(1) AS item_count
    FOR status IN ('REFUNDED' AS refunded, 'SUCCESS' AS success)
)
