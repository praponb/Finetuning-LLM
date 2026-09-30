WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
),
scored AS (
    SELECT
        id,
        plan,
        currency,
        val,
        RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS LAST) AS val_rank,
        RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS LAST) AS val_rank_2
    FROM lines
),
valid AS (
    SELECT *, ROUND(val_rank_2, 2) AS val_rank_2_rounded
    FROM scored
    WHERE val < 24
),
metrics AS (
    SELECT
        *,
        MIN(val) OVER (PARTITION BY plan) AS plan_min_val,
        PERCENT_RANK() OVER (PARTITION BY currency, plan ORDER BY val NULLS FIRST) AS val_percent_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val_rank_2_rounded DESC) AS val_rank_2_rounded_dense_rank
    FROM valid
),
scored_price_tiers AS (
    SELECT
        id,
        plan,
        currency,
        val,
        val_rank,
        val_rank_2,
        val_rank_2_rounded,
        plan_min_val,
        val_percent_rank,
        val_rank_2_rounded_dense_rank,
        ROUND(val_rank_2_rounded_dense_rank / SUM(val_rank_2_rounded_dense_rank) OVER (PARTITION BY currency), 4) AS val_rank_2_rounded_dense_rank_share,
        MIN(val_percent_rank) OVER (PARTITION BY currency) AS currency_min_val_percent_rank
    FROM metrics
),
cleaned AS (
    SELECT *, CAST(currency_min_val_percent_rank AS DOUBLE) AS currency_min_val_percent_rank_num
    FROM scored_price_tiers
    WHERE currency_min_val_percent_rank > 1
)
SELECT
    id,
    val,
    val_rank_2_rounded,
    plan_min_val,
    val_percent_rank,
    currency_min_val_percent_rank,
    currency_min_val_percent_rank_num
FROM cleaned
ORDER BY val_percent_rank DESC NULLS LAST;
