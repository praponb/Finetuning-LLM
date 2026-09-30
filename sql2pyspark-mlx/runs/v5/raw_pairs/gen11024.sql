WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS element
),
matched_keys AS (
    -- Set difference on keys
    SELECT plan
    FROM flattened
    EXCEPT
    SELECT plan
    FROM price_tiers
    WHERE effective_from >= '2025-02-11'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM flattened s
    INNER JOIN matched_keys k ON s.plan = k.plan
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        effective_from,
        currency,
        element,
        DATE_TRUNC('year', effective_from) AS effective_from_period,
        CONCAT_WS('/', currency, plan) AS currency_plan_key
    FROM reconciled
    WHERE effective_from BETWEEN '2023-05-01' AND '2023-08-06'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        effective_from,
        currency_plan_key,
        MIN(element) OVER w_hist AS currency_min_element
    FROM valid
    WHERE effective_from > '2023-08-01'
    WINDOW w_hist AS (PARTITION BY currency)
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        effective_from,
        currency_plan_key,
        CUME_DIST() OVER (ORDER BY currency_min_element NULLS LAST) AS currency_min_element_cume_dist,
        PERCENT_RANK() OVER (ORDER BY currency_min_element NULLS LAST) AS currency_min_element_percent_rank
    FROM scored
)
SELECT
    effective_from,
    currency_plan_key,
    currency_min_element_cume_dist
FROM metrics
ORDER BY effective_from DESC NULLS FIRST;
