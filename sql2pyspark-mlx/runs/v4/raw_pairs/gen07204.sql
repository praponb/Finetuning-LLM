WITH exploded AS (
    SELECT
        id,
        plan,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS val
),
metrics AS (
    SELECT
        id,
        plan,
        val,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY val ASC) AS val_percent_rank,
        PERCENT_RANK() OVER (ORDER BY val DESC) AS val_percent_rank_2,
        MIN(val) OVER (PARTITION BY plan) AS plan_min_val,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS FIRST) AS val_dense_rank,
        RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) AS val_rank
    FROM exploded
),
cleaned AS (
    SELECT
        id,
        plan,
        val,
        val_percent_rank,
        val_percent_rank_2,
        val_rank,
        val_dense_rank / NULLIF(val_percent_rank, 0) AS val_dense_rank_per_val_percent_rank,
        val_rank - val_percent_rank_2 AS val_rank_minus_val_percent_rank_2
    FROM metrics
    WHERE plan IN ('basic', 'enterprise')
)
SELECT
    plan,
    val,
    val_percent_rank_2,
    val_rank
FROM cleaned
ORDER BY val ASC
