WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS val
)
-- Ranking and gap features
SELECT
    id,
    effective_from,
    STDDEV_SAMP(val) OVER (PARTITION BY currency) AS currency_stddev_samp_val,
    CUME_DIST() OVER (PARTITION BY currency ORDER BY val DESC NULLS LAST) AS val_cume_dist,
    DENSE_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS FIRST) AS val_dense_rank,
    COUNT(val) OVER (PARTITION BY currency) AS currency_count_val
FROM lines
