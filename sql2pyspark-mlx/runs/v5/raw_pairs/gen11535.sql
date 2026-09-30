WITH valid AS (
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        ADD_MONTHS(ends_on, 12) ends_on_plus_months
    FROM promotions
    WHERE starts_on > '2025-04-01'
)
SELECT
    *,
    ROW_NUMBER() OVER (ORDER BY ends_on_plus_months, id) seq_no,
    AVG(discount_pct) OVER (ORDER BY discount_pct, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) discount_pct_avg_2,
    SUM(discount_pct) OVER (ORDER BY ends_on_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_discount_pct,
    SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount_pct,
    SUM(discount_pct) OVER (PARTITION BY promo_code ORDER BY ends_on_plus_months, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_discount_pct_2
FROM valid;
