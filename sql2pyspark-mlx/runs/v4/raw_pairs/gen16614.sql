WITH flattened AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS tier_value
    FROM price_tiers
),
base AS (
    SELECT *, LENGTH(currency) AS currency_length
    FROM flattened
    WHERE effective_from BETWEEN '2024-10-01' AND '2025-04-01'
),
scored AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY currency_length DESC NULLS LAST) AS currency_length_dense_rank,
        MIN(tier_value) OVER (PARTITION BY currency) AS currency_min_tier_value,
        CUME_DIST() OVER w_running AS currency_length_cume_dist,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value NULLS LAST) AS tier_value_dense_rank
    FROM base
    WINDOW w_running AS (PARTITION BY currency ORDER BY currency_length DESC)
),
enriched AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_value,
        currency_length,
        currency_length_dense_rank,
        currency_min_tier_value,
        currency_length_cume_dist,
        tier_value_dense_rank,
        STDDEV_SAMP(currency_length_dense_rank) OVER (PARTITION BY currency) AS currency_stddev_samp_currency_length_dense_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank_2
    FROM scored
)
SELECT
    currency,
    APPROX_PERCENTILE(tier_value_dense_rank_2, ARRAY(0.25, 0.5, 0.75)) AS tier_value_dense_rank_2_quartiles,
    MAX(currency_length_dense_rank) FILTER (WHERE currency = 'THB') AS thb_max_currency_length_dense_rank
FROM enriched
GROUP BY 1
HAVING thb_max_currency_length_dense_rank < 0
ORDER BY thb_max_currency_length_dense_rank;
