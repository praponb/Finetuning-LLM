WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(amount, 0) AS amount_rounded,
        COALESCE(method, 'OTHER') AS resolved_method
    FROM payments
    WHERE paid_at > TO_DATE('2023-05-16')
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY paid_at DESC, id) AS rn
        FROM cleaned
    ) ranked
    WHERE rn = 1
),
deduped_payments AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, RANK() OVER (PARTITION BY method ORDER BY amount DESC) AS rank_in_group
        FROM deduped
    ) ranked
    WHERE rank_in_group = 1
)
-- Window metrics over several different partitions
SELECT
    *,
    CUME_DIST() OVER (ORDER BY amount_rounded DESC NULLS FIRST) AS amount_rounded_cume_dist,
    LAST_VALUE(amount_rounded, TRUE) OVER (PARTITION BY method ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount_rounded
FROM deduped_payments;
