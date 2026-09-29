WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS val
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, MONTH(effective_from) AS effective_from_month
    FROM flattened
    WHERE effective_from >= DATE '2024-06-09'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        val,
        effective_from_month,
        PERCENT_RANK() OVER (ORDER BY val DESC) AS val_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS LAST) AS val_percent_rank_2,
        STDDEV_SAMP(effective_from_month) OVER (PARTITION BY currency) AS currency_stddev_samp_effective_from_month,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS LAST) AS val_percent_rank_3
    FROM filtered
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY effective_from_month) AS effective_from_month_percent_rank,
        CUME_DIST() OVER (ORDER BY val_percent_rank DESC) AS val_percent_rank_cume_dist,
        DENSE_RANK() OVER (ORDER BY effective_from_month DESC NULLS FIRST) AS effective_from_month_dense_rank
    FROM enriched
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_windows
    INTERSECT ALL
    SELECT id
    FROM price_tiers
    WHERE currency != 'USD'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN matched_keys k ON s.id = k.id
)
SELECT
    id,
    effective_from,
    effective_from_month,
    val_percent_rank,
    currency_stddev_samp_effective_from_month,
    val_percent_rank_3,
    effective_from_month_percent_rank,
    val_percent_rank_cume_dist
FROM verified
ORDER BY currency_stddev_samp_effective_from_month DESC NULLS LAST;
