WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN promo_code IN ('WELCOME5', 'BUNDLE') THEN 'Core' ELSE 'Other' END AS promo_code_group,
        COALESCE(channel, 'NONE') AS resolved_channel
    FROM promotions
    WHERE channel IS NOT NULL
      AND starts_on <= '2023-03-01'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(discount_pct, 1) OVER (ORDER BY starts_on, id) AS prev_discount_pct,
        AVG(discount_pct) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_pct_avg_6,
        ROUND(discount_pct / SUM(discount_pct) OVER (PARTITION BY promo_code, channel), 4) AS discount_pct_share
    FROM base
),
deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct_share DESC NULLS LAST, id) AS latest_rank
        FROM scored
    ) ranked
    WHERE latest_rank = 1
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAST_VALUE(discount_pct_avg_6) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY discount_pct_share, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount_pct_avg_6,
        SUM(discount_pct_avg_6) OVER (PARTITION BY promo_code, channel ORDER BY discount_pct_avg_6, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct_avg_6
    FROM deduped
    WHERE starts_on < '2025-02-08'
),
totals AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('MONTH', ends_on) AS activity_month,
        SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list,
        SORT_ARRAY(COLLECT_SET(promo_code)) AS promo_code_list_2,
        ROUND(SUM(last_known_discount_pct_avg_6) / NULLIF(COUNT(CASE WHEN channel = 'partner' THEN 1 END), 0), 2) AS last_known_discount_pct_avg_6_per_partner,
        COUNT(CASE WHEN promo_code = 'WELCOME5' THEN 1 END) AS welcome5_count,
        SIZE(COLLECT_SET(promo_code)) AS num_promo_codes,
        COUNT_IF(channel = 'mobile') AS mobile_events,
        GROUPING_ID() AS grouping_id
    FROM enriched
    GROUP BY CUBE (DATE_TRUNC('MONTH', ends_on))
)
SELECT
    promo_code_list,
    promo_code_list_2,
    welcome5_count,
    num_promo_codes,
    mobile_events,
    grouping_id
FROM totals
ORDER BY welcome5_count NULLS LAST;
