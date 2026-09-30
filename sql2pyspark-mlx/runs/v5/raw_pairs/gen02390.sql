WITH lines AS (
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) e AS val
),
scored AS (
    SELECT *, STDDEV_SAMP(val) OVER w_running currency_stddev_samp_val
    FROM lines
    WINDOW w_running AS (PARTITION BY currency)
),
with_windows AS (
    SELECT
        *,
        MAX(val) OVER (PARTITION BY currency) currency_max_val,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY currency_stddev_samp_val NULLS FIRST) currency_stddev_samp_val_percent_rank
    FROM scored
),
filtered AS (
    SELECT
        id,
        effective_from,
        val,
        currency_stddev_samp_val,
        currency_max_val,
        currency_stddev_samp_val_percent_rank,
        UPPER(currency) currency_upper
    FROM with_windows
    WHERE effective_from > DATE '2024-06-13'
      AND currency_max_val > 46
),
scored_price_tiers AS (
    SELECT
        id,
        val,
        currency_max_val,
        currency_stddev_samp_val_percent_rank,
        currency_upper,
        RANK() OVER (ORDER BY currency_stddev_samp_val DESC NULLS FIRST) currency_stddev_samp_val_rank,
        PERCENT_RANK() OVER (ORDER BY val DESC) val_percent_rank
    FROM filtered
    WHERE currency_stddev_samp_val BETWEEN 21 AND 40
),
filtered_price_tiers AS (
    SELECT
        id,
        val,
        currency_max_val,
        currency_upper,
        currency_stddev_samp_val_rank,
        val_percent_rank,
        CASE WHEN currency_stddev_samp_val_rank >= 8 THEN 'High' WHEN currency_stddev_samp_val_rank >= 4 THEN 'Medium' ELSE 'Low' END currency_stddev_samp_val_rank_band
    FROM scored_price_tiers
    WHERE currency_stddev_samp_val_rank >= 4
),
daily AS (
    SELECT
        CASE WHEN val_percent_rank >= 0.86 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_percent_rank_tier,
        COUNT(*) AS num_rows,
        PERCENTILE_APPROX(val, 0.95) AS val_p95,
        COUNT(*) AS row_count,
        PERCENTILE_APPROX(currency_stddev_samp_val_rank, 0.5) AS currency_stddev_samp_val_rank_p50,
        COUNT(*) AS row_count_2,
        APPROX_PERCENTILE(currency_stddev_samp_val_rank, ARRAY(0.25, 0.5, 0.75)) AS currency_stddev_samp_val_rank_quartiles
    FROM filtered_price_tiers
    GROUP BY CASE WHEN val_percent_rank >= 0.86 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    val_percent_rank_tier,
    num_rows,
    row_count,
    currency_stddev_samp_val_rank_p50,
    row_count_2,
    currency_stddev_samp_val_rank_quartiles
FROM daily
ORDER BY row_count DESC NULLS FIRST
