WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE_OUTER(discounts) AS val
    FROM price_tiers
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT *, CAST(val AS INT) val_int
    FROM exploded
    WHERE currency = 'EUR'
),
filtered_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, val_int * val val_int_times_val
    FROM filtered
    WHERE currency <> 'EUR'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(val_int_times_val) OVER (PARTITION BY currency) currency_max_val_int_times_val,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val DESC NULLS LAST) val_cume_dist,
        RANK() OVER w_grp val_int_rank
    FROM filtered_price_tiers
    WINDOW w_grp AS (PARTITION BY currency ORDER BY val_int DESC)
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(currency_max_val_int_times_val) OVER w_running currency_avg_currency_max_val_int_times_val,
        STDDEV_SAMP(val_int) OVER w_running currency_stddev_samp_val_int
    FROM scored
    WINDOW w_running AS (PARTITION BY currency)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        val_int_times_val,
        currency_max_val_int_times_val,
        val_cume_dist,
        currency_avg_currency_max_val_int_times_val,
        currency_stddev_samp_val_int,
        DENSE_RANK() OVER w_running val_dense_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val) val_dense_rank_2
    FROM scored_price_tiers
    WINDOW w_running AS (PARTITION BY currency ORDER BY val DESC NULLS FIRST)
)
-- Running and moving aggregates
SELECT
    val_cume_dist,
    currency_avg_currency_max_val_int_times_val,
    val_dense_rank,
    val_dense_rank_2,
    DENSE_RANK() OVER (ORDER BY currency_avg_currency_max_val_int_times_val DESC NULLS FIRST) currency_avg_currency_max_val_int_times_val_dense_rank,
    DENSE_RANK() OVER (ORDER BY val_dense_rank DESC) val_dense_rank_dense_rank,
    PERCENT_RANK() OVER w_recent val_dense_rank_2_percent_rank,
    CUME_DIST() OVER w_recent val_dense_rank_2_cume_dist,
    CUME_DIST() OVER w_recent_2 val_int_times_val_cume_dist
FROM with_windows
WINDOW w_recent AS (ORDER BY val_dense_rank_2), w_recent_2 AS (ORDER BY val_int_times_val DESC NULLS LAST);
