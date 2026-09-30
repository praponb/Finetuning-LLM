WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LENGTH(channel) AS channel_length,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM promotions
    WHERE channel IN ('store', 'mobile')
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on DESC, id) AS rn
    FROM filtered
    QUALIFY rn = 1
),
latest_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY channel_length NULLS FIRST, id) AS rn_2
        FROM latest
    ) ranked
    WHERE rn_2 = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        discount_pct,
        resolved_channel,
        rn,
        rn_2,
        ADD_MONTHS(starts_on, 12) AS starts_on_plus_months,
        CONCAT_WS('/', channel, promo_code) AS channel_promo_code_key
    FROM latest_promotions
    WHERE starts_on > DATE '2023-08-25'
      AND discount_pct BETWEEN 0.18 AND 0.2
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY rn_2, id) AS dedup_rank
    FROM base
    QUALIFY dedup_rank = 1
),
base_promotions AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        rn + rn_2 AS rn_plus_rn_2,
        DATE_ADD(starts_on, 56) AS starts_on_plus_56d
    FROM deduped
    WHERE starts_on >= TO_DATE('2023-07-22')
),
deduped_promotions AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY rn_plus_rn_2 DESC NULLS FIRST, id) AS latest_rank
        FROM base_promotions
    ) t
    WHERE latest_rank = 1
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('MONTH', starts_on_plus_months) AS activity_month,
    CASE WHEN rn_plus_rn_2 >= 84 THEN 'HIGH' ELSE 'LOW' END AS rn_plus_rn_2_tier,
    MAX(rn_plus_rn_2) FILTER (WHERE channel = 'store') AS store_max_rn_plus_rn_2,
    APPROX_PERCENTILE(rn_plus_rn_2, ARRAY(0.25, 0.5, 0.75)) AS rn_plus_rn_2_quartiles,
    COUNT(CASE WHEN channel = 'store' THEN 1 END) AS store_count
FROM deduped_promotions
GROUP BY DATE_TRUNC('MONTH', starts_on_plus_months), CASE WHEN rn_plus_rn_2 >= 84 THEN 'HIGH' ELSE 'LOW' END
