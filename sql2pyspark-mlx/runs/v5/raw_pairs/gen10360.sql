WITH flattened AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS element
    FROM price_tiers
),
metrics AS (
    SELECT
        id,
        currency,
        element,
        RANK() OVER (PARTITION BY currency ORDER BY element DESC NULLS LAST) AS element_rank,
        SUM(element) OVER (PARTITION BY currency) AS currency_sum_element
    FROM flattened
),
filtered AS (
    SELECT *, INITCAP(TRIM(currency)) AS currency_title
    FROM metrics
    WHERE element_rank = 4
),
metrics_price_tiers AS (
    SELECT
        *,
        MAX(element) OVER w_hist AS currency_max_element,
        STDDEV_SAMP(element) OVER w_hist AS currency_stddev_samp_element,
        PERCENT_RANK() OVER w_recent AS currency_sum_element_percent_rank,
        SUM(currency_sum_element) OVER w_hist AS currency_sum_currency_sum_element,
        MIN(currency_sum_element) OVER w_hist AS currency_min_currency_sum_element
    FROM filtered
    WINDOW w_hist AS (PARTITION BY currency), w_recent AS (PARTITION BY currency ORDER BY currency_sum_element NULLS LAST)
),
with_windows AS (
    SELECT
        id,
        currency,
        element,
        element_rank,
        currency_sum_element,
        currency_title,
        currency_stddev_samp_element,
        currency_sum_element_percent_rank,
        currency_sum_currency_sum_element,
        currency_min_currency_sum_element,
        SUM(currency_max_element) OVER w_running AS currency_sum_currency_max_element,
        AVG(currency_sum_element) OVER w_running AS currency_avg_currency_sum_element
    FROM metrics_price_tiers
    WINDOW w_running AS (PARTITION BY currency)
),
valid AS (
    SELECT *, ROUND(currency_sum_element_percent_rank * 0.9, 2) AS currency_sum_element_percent_rank_scaled
    FROM with_windows
    WHERE currency_sum_element <= 150
)
SELECT
    currency_title,
    currency_sum_currency_sum_element,
    currency_min_currency_sum_element,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY currency_sum_currency_max_element DESC NULLS FIRST) AS currency_sum_currency_max_element_percent_rank,
    COUNT(currency_sum_currency_sum_element) OVER (PARTITION BY currency) AS currency_count_currency_sum_currency_sum_element
FROM valid;
