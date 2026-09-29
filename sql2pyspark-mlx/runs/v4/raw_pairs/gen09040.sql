WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(ends_on, starts_on) AS days_starts_on_to_ends_on,
        CASE WHEN channel = 'store' THEN 1 ELSE 0 END AS is_store,
        COALESCE(channel, 'DIRECT') AS resolved_channel
    FROM promotions
    WHERE channel IS NOT NULL
)
-- Running and moving aggregates
SELECT
    *,
    FIRST_VALUE(days_starts_on_to_ends_on, TRUE) OVER (PARTITION BY promo_code ORDER BY days_starts_on_to_ends_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_days_starts_on_to_ends_on,
    SUM(is_store) OVER (PARTITION BY promo_code, channel ORDER BY starts_on ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_is_store
FROM cleaned
