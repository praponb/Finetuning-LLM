WITH kept_keys AS (
    -- Set difference on keys
    SELECT promo_code, id
    FROM promotions
    EXCEPT ALL
    SELECT promo_code, id
    FROM promotions
    WHERE ends_on BETWEEN '2023-04-25' AND '2025-03-13'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM promotions s
    INNER JOIN kept_keys k ON s.promo_code = k.promo_code AND s.id = k.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(channel, 'unassigned') channel_filled
    FROM verified
    WHERE discount_pct BETWEEN 0.13 AND 0.26
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code, channel), 4) discount_pct_share,
        DENSE_RANK() OVER (ORDER BY discount_pct DESC) discount_pct_dense_rank,
        AVG(discount_pct) OVER (PARTITION BY promo_code) promo_code_avg_discount_pct
    FROM base
),
pivoted AS (
    -- Native SQL PIVOT
    SELECT *
    FROM (
        SELECT
            channel,
            promo_code,
            discount_pct
        FROM enriched
    ) src
    PIVOT (
        SUM(discount_pct) AS amount
        FOR promo_code IN ('SPRING10', 'WELCOME5')
    )
)
SELECT channel
FROM pivoted;
