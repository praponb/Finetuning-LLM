WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        CAST(discount_pct AS INT) AS discount_pct_int
    FROM promotions
    WHERE ends_on >= DATE '2023-06-23'
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT promo_code
    FROM base
    INTERSECT ALL
    SELECT promo_code
    FROM promotions
    WHERE discount_pct >= 0.14
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN remaining_keys k ON s.promo_code = k.promo_code
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(promo_code) AS promo_code_upper,
        COALESCE(channel, 'OTHER') AS resolved_channel
    FROM reconciled
    WHERE ends_on >= DATE '2023-06-13'
      AND discount_pct_int = 50
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        UPPER(channel) AS channel_upper,
        COALESCE(channel, 'N/A') AS channel_filled
    FROM cleaned
    WHERE discount_pct_int BETWEEN 47 AND 59
),
kept_keys AS (
    -- Set intersection on keys
    SELECT channel, promo_code
    FROM filtered
    INTERSECT ALL
    SELECT channel, promo_code
    FROM promotions
    WHERE starts_on < '2025-07-01'
),
reconciled_promotions AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    JOIN kept_keys k ON s.channel = k.channel AND s.promo_code = k.promo_code
)
-- Grouped metrics
SELECT
    channel,
    DATE_TRUNC('YEAR', ends_on) AS year_start,
    CASE WHEN discount_pct >= 0.28 THEN 'HIGH' ELSE 'LOW' END AS discount_pct_tier,
    SUM(CASE WHEN promo_code = 'SPRING10' THEN discount_pct_int ELSE 0 END) AS spring10_discount_pct_int,
    MAX(discount_pct) AS max_discount_pct,
    SUM(discount_pct) AS total_discount_pct,
    RANK() OVER (PARTITION BY channel ORDER BY SUM(discount_pct) DESC) AS total_discount_pct_rank
FROM reconciled_promotions
GROUP BY channel, DATE_TRUNC('YEAR', ends_on), CASE WHEN discount_pct >= 0.28 THEN 'HIGH' ELSE 'LOW' END
ORDER BY discount_pct_tier DESC NULLS LAST
