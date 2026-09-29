WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(discounts) AS element
    FROM price_tiers
),
with_windows AS (
    SELECT *, RANK() OVER w_running AS element_rank
    FROM lines
    WHERE currency = 'EUR'
    WINDOW w_running AS (PARTITION BY plan ORDER BY element DESC NULLS FIRST)
),
enriched AS (
    SELECT
        effective_from,
        currency,
        STDDEV_SAMP(element_rank) OVER (PARTITION BY currency) AS currency_stddev_samp_element_rank,
        CUME_DIST() OVER (ORDER BY element_rank DESC NULLS LAST) AS element_rank_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY element NULLS LAST) AS element_rank_2,
        PERCENT_RANK() OVER (ORDER BY element_rank DESC) AS element_rank_percent_rank,
        DENSE_RANK() OVER (PARTITION BY currency, plan ORDER BY element_rank DESC NULLS FIRST) AS element_rank_dense_rank
    FROM with_windows
),
cleaned AS (
    SELECT
        *,
        CAST(element_rank_2 AS DOUBLE) AS element_rank_2_str,
        LEAST(element_rank_percent_rank, currency_stddev_samp_element_rank) AS least_element_rank_percent_rank_currency_stddev_samp_element_rank
    FROM enriched
    WHERE effective_from >= DATE '2024-03-24'
      AND element_rank_dense_rank >= 3
)
SELECT
    effective_from,
    currency,
    currency_stddev_samp_element_rank,
    element_rank_cume_dist,
    element_rank_dense_rank,
    element_rank_2_str,
    CASE WHEN currency RLIKE '^(USD|EUR)$' THEN 'CORE' WHEN currency RLIKE '^(THB)$' THEN 'GROUP_A' ELSE 'PRIORITY' END AS currency_zone
FROM cleaned
ORDER BY element_rank_dense_rank DESC NULLS LAST;
