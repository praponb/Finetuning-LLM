WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) li AS val
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, ROUND(val * 0.9, 2) AS val_scaled
    FROM exploded
    WHERE effective_from > TO_DATE('2024-04-09')
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        effective_from,
        currency,
        PERCENT_RANK() OVER (ORDER BY val DESC) AS val_percent_rank,
        RANK() OVER w_part AS val_scaled_rank,
        AVG(val) OVER (PARTITION BY currency) AS currency_avg_val,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val_scaled) AS val_scaled_percent_rank
    FROM base
    WINDOW w_part AS (PARTITION BY currency ORDER BY val_scaled DESC)
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        effective_from,
        currency,
        val_percent_rank,
        val_scaled_percent_rank,
        currency_avg_val + val_percent_rank AS currency_avg_val_plus_val_percent_rank,
        DAYOFWEEK(effective_from) AS effective_from_dayofweek
    FROM enriched
    WHERE effective_from >= DATE '2024-02-11'
),
valid_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        effective_from,
        currency,
        val_percent_rank,
        val_scaled_percent_rank,
        effective_from_dayofweek,
        val_scaled_percent_rank * val_percent_rank AS val_scaled_percent_rank_times_val_percent_rank,
        COALESCE(currency_avg_val_plus_val_percent_rank, 0) AS currency_avg_val_plus_val_percent_rank_filled
    FROM valid
    WHERE effective_from >= DATE '2024-05-28'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT currency
    FROM valid_price_tiers
    INTERSECT
    SELECT currency
    FROM price_tiers
    WHERE currency NOT IN ('EUR', 'THB')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid_price_tiers s
    INNER JOIN kept_keys k ON s.currency = k.currency
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
        PERCENTILE_APPROX(val_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS val_percent_rank_quartiles,
        SUM(CASE WHEN currency = 'EUR' THEN currency_avg_val_plus_val_percent_rank_filled ELSE 0 END) AS eur_currency_avg_val_plus_val_percent_rank_filled,
        PERCENTILE_APPROX(val_percent_rank, 0.9) AS val_percent_rank_p90
    FROM kept_rows
    GROUP BY 1
    HAVING val_percent_rank_p90 >= 0
)
SELECT
    quarter_start,
    val_percent_rank_quartiles,
    eur_currency_avg_val_plus_val_percent_rank_filled,
    val_percent_rank_p90
FROM grouped
