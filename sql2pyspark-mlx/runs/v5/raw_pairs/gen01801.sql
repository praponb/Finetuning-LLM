WITH cleaned AS (
    SELECT
        id,
        channel,
        promo_code,
        starts_on,
        ends_on,
        discount_pct,
        DATE_TRUNC('month', starts_on) AS starts_on_period
    FROM promotions
    WHERE starts_on > DATE '2023-10-11'
),
filtered AS (
    SELECT
        id,
        channel,
        starts_on,
        ends_on,
        discount_pct,
        starts_on_period,
        LAST_DAY(starts_on) AS starts_on_month_end
    FROM cleaned
    WHERE ends_on >= DATE '2023-08-19'
      AND channel IS NOT NULL
),
gaps AS (
    SELECT *, CASE WHEN LAG(starts_on_period) OVER (PARTITION BY channel ORDER BY starts_on_period, id) IS NULL
                 OR starts_on_period > LAG(starts_on_period) OVER (PARTITION BY channel ORDER BY starts_on_period, id) + INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM filtered
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY channel ORDER BY starts_on_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
matched AS (
    SELECT
        src.*,
        pr.id AS promo_id,
        pr.promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM islands src
    LEFT JOIN promotions pr
        ON src.channel = pr.channel
       AND src.starts_on_month_end >= pr.starts_on
       AND src.starts_on_month_end < pr.ends_on + INTERVAL '7' DAY
)
SELECT
    channel,
    MAX(starts_on_period) AS last_starts_on_period,
    MIN(starts_on_period) AS first_seen,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN promo_code = 'BUNDLE' THEN 1 END), 0), 2) AS discount_pct_per_bundle
FROM matched
GROUP BY 1;
