WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS element
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER w_running AS element_percent_rank,
        PERCENT_RANK() OVER w_running AS element_percent_rank_2,
        STDDEV_SAMP(element) OVER w_running_2 AS currency_stddev_samp_element
    FROM lines
    WINDOW w_running AS (ORDER BY element), w_running_2 AS (PARTITION BY currency)
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT *, SUM(element_percent_rank) OVER (PARTITION BY currency, plan) AS currency_sum_element_percent_rank
    FROM scored
),
scored_price_tiers_2 AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY currency, plan ORDER BY element_percent_rank_2 NULLS FIRST) AS element_percent_rank_2_cume_dist,
        SUM(element_percent_rank) OVER w_recent AS currency_sum_element_percent_rank_2
    FROM scored_price_tiers
    WINDOW w_recent AS (PARTITION BY currency, plan)
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    plan,
    measure,
    value
FROM (
    SELECT
        plan,
        element,
        element_percent_rank_2,
        element_percent_rank,
        element_percent_rank_2_cume_dist
    FROM scored_price_tiers_2
) src
UNPIVOT (
    value FOR measure IN (element, element_percent_rank_2, element_percent_rank, element_percent_rank_2_cume_dist)
);
