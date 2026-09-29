WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LAST_DAY(ends_on) AS ends_on_month_end
    FROM promotions
    WHERE ends_on >= DATE '2023-10-19'
)
-- Running and moving aggregates
SELECT
    id,
    promo_code,
    ends_on,
    ends_on_month_end,
    FIRST_VALUE(channel) IGNORE NULLS OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_channel,
    LAG(discount_pct, 1) OVER w_recent AS prev_discount_pct,
    SUM(discount_pct) OVER w_hist AS cumulative_discount_pct
FROM valid
WINDOW w_recent AS (PARTITION BY channel ORDER BY discount_pct, id), w_hist AS (PARTITION BY channel ORDER BY ends_on_month_end, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW);
