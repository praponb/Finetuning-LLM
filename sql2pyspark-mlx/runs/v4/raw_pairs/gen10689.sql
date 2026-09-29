WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS val
    FROM price_tiers
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        currency,
        val,
        STDDEV_SAMP(val) OVER w_grp AS currency_stddev_samp_val,
        MAX(val) OVER w_grp AS currency_max_val,
        RANK() OVER w_grp_2 AS val_rank,
        DENSE_RANK() OVER w_grp_3 AS val_dense_rank
    FROM lines
    WINDOW w_grp AS (PARTITION BY currency), w_grp_2 AS (PARTITION BY currency ORDER BY val NULLS LAST), w_grp_3 AS (PARTITION BY currency ORDER BY val DESC NULLS FIRST)
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, GREATEST(currency_stddev_samp_val, val_dense_rank) AS greatest_currency_stddev_samp_val_val_dense_rank
    FROM with_windows
    WHERE effective_from > DATE '2024-03-02'
      AND val_rank IS NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        effective_from,
        val,
        currency_stddev_samp_val,
        currency_max_val,
        val_rank,
        val_dense_rank,
        COALESCE(val_dense_rank, 0) AS val_dense_rank_filled,
        DATE_FORMAT(effective_from, 'yyyy-MM') AS effective_from_label
    FROM valid
    WHERE effective_from >= TO_DATE('2024-04-01')
      AND currency_stddev_samp_val = 66
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        effective_from,
        val,
        currency_stddev_samp_val,
        currency_max_val,
        val_rank,
        val_dense_rank_filled,
        effective_from_label,
        val * currency_max_val AS val_times_currency_max_val,
        FLOOR(val) AS val_whole
    FROM cleaned
    WHERE effective_from >= '2025-01-01'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN val_dense_rank_filled >= 67 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_dense_rank_filled_tier,
        PERCENTILE_APPROX(val, 0.5) AS val_p50,
        PERCENTILE_APPROX(currency_stddev_samp_val, 0.9) AS currency_stddev_samp_val_p90,
        APPROX_PERCENTILE(val, ARRAY(0.25, 0.5, 0.75)) AS val_quartiles,
        ROUND(AVG(currency_stddev_samp_val), 2) AS avg_currency_stddev_samp_val,
        COUNT(*) AS num_rows
    FROM filtered
    GROUP BY 1
    HAVING currency_stddev_samp_val_p90 >= 0
)
SELECT
    val_dense_rank_filled_tier,
    currency_stddev_samp_val_p90,
    val_quartiles,
    avg_currency_stddev_samp_val,
    num_rows
FROM totals
ORDER BY avg_currency_stddev_samp_val NULLS FIRST;
