WITH base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CONCAT(promo_code, ' ', channel) AS promo_code_channel,
        LENGTH(promo_code) AS promo_code_length
    FROM promotions
    WHERE channel IS NOT NULL
      AND discount_pct >= 0.19
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct,
        promo_code_length,
        CAST(discount_pct AS INT) AS discount_pct_int,
        CAST(discount_pct AS INT) AS discount_pct_int_2,
        COALESCE(channel, 'OTHER') AS channel_filled
    FROM base
    WHERE channel IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct,
        discount_pct_int,
        discount_pct_int_2,
        ADD_MONTHS(starts_on, 1) AS starts_on_plus_months,
        COALESCE(discount_pct_int_2, 0) AS discount_pct_int_2_filled
    FROM valid
    WHERE promo_code = 'WELCOME5'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        starts_on,
        discount_pct,
        discount_pct_int,
        discount_pct_int_2,
        starts_on_plus_months,
        discount_pct_int_2_filled,
        AVG(discount_pct_int) OVER (ORDER BY discount_pct_int_2, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS discount_pct_int_avg_5,
        SUM(discount_pct_int_2) OVER (PARTITION BY promo_code ORDER BY starts_on_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct_int_2,
        FIRST_VALUE(discount_pct_int) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY starts_on_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_discount_pct_int
    FROM cleaned
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct_int_2 DESC NULLS FIRST, id) AS latest_rank
        FROM with_windows
    ) t
    WHERE latest_rank = 1
),
with_windows_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_recent AS first_known_discount_pct_int_rank,
        AVG(discount_pct_int) OVER (PARTITION BY promo_code ORDER BY running_discount_pct_int_2, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS discount_pct_int_avg_6,
        AVG(running_discount_pct_int_2) OVER (PARTITION BY promo_code) AS promo_code_avg_running_discount_pct_int_2,
        SUM(discount_pct_int_avg_5) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct_int_avg_5
    FROM deduped
    WINDOW w_recent AS (ORDER BY first_known_discount_pct_int NULLS LAST)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(discount_pct_int, TRUE) OVER (ORDER BY promo_code_avg_running_discount_pct_int_2, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_discount_pct_int,
        LAST_VALUE(running_discount_pct_int_2, TRUE) OVER (PARTITION BY promo_code ORDER BY first_known_discount_pct_int_rank, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_running_discount_pct_int_2
    FROM with_windows_promotions
)
SELECT
    id,
    promo_code,
    starts_on,
    discount_pct,
    starts_on_plus_months,
    discount_pct_int_avg_5,
    latest_rank,
    discount_pct_int_avg_6,
    promo_code_avg_running_discount_pct_int_2
FROM enriched
ORDER BY starts_on_plus_months
