WITH exploded AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS tier_value
),
cleaned AS (
    SELECT *, DATE_TRUNC('month', effective_from) AS effective_from_period
    FROM exploded
    WHERE tier_value < 28
),
scored AS (
    SELECT *, DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_dense_rank
    FROM cleaned
),
enriched AS (
    SELECT
        currency,
        tier_value,
        effective_from_period,
        tier_value_dense_rank,
        ROUND(tier_value_dense_rank / SUM(tier_value_dense_rank) OVER (PARTITION BY currency), 4) AS tier_value_dense_rank_share,
        ROUND(tier_value_dense_rank / SUM(tier_value_dense_rank) OVER (PARTITION BY currency), 4) AS tier_value_dense_rank_share_2,
        MAX(tier_value) OVER (PARTITION BY currency) AS currency_max_tier_value
    FROM scored
    WHERE tier_value BETWEEN 26 AND 66
),
scored_price_tiers AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY currency_max_tier_value ASC) AS currency_max_tier_value_dense_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value_dense_rank_share_2 DESC NULLS FIRST) AS tier_value_dense_rank_share_2_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank
    FROM enriched
),
daily AS (
    SELECT
        DATE_TRUNC('WEEK', effective_from_period) AS week_start,
        CASE WHEN tier_value_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_dense_rank_tier,
        APPROX_PERCENTILE(currency_max_tier_value_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS currency_max_tier_value_dense_rank_quartiles,
        SIZE(COLLECT_SET(currency)) AS num_currencies,
        COUNT(DISTINCT currency) AS distinct_currencies,
        GROUPING_ID() AS grouping_level
    FROM scored_price_tiers
    GROUP BY CUBE (DATE_TRUNC('WEEK', effective_from_period), CASE WHEN tier_value_dense_rank >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END)
    HAVING distinct_currencies >= 1
)
SELECT
    week_start,
    num_currencies,
    distinct_currencies,
    grouping_level
FROM daily
ORDER BY grouping_level DESC, week_start ASC
