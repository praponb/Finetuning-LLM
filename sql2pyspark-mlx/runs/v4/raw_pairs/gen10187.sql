WITH filtered AS (
    SELECT
        id,
        channel,
        promo_code,
        ends_on,
        discount_pct,
        CAST(discount_pct AS INT) AS discount_pct_int
    FROM promotions
    WHERE ends_on > DATE '2024-04-04'
      AND channel IS NOT NULL
      AND promo_code NOT IN ('BUNDLE', 'WELCOME5')
),
base AS (
    SELECT *, DATE_TRUNC('WEEK', ends_on) AS ends_on_period
    FROM filtered
    WHERE channel IS NOT NULL
),
metrics AS (
    SELECT
        *,
        COUNT(discount_pct_int) OVER w_ordered AS promo_code_count_discount_pct_int,
        SUM(discount_pct_int) OVER w_ordered_2 AS cumulative_discount_pct_int,
        ROW_NUMBER() OVER w_recent AS seq_no
    FROM base
    WINDOW w_ordered AS (PARTITION BY promo_code), w_ordered_2 AS (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent AS (PARTITION BY channel ORDER BY ends_on_period, id)
)
SELECT
    id,
    channel,
    promo_code,
    ends_on,
    discount_pct,
    discount_pct_int,
    ends_on_period,
    promo_code_count_discount_pct_int,
    cumulative_discount_pct_int,
    seq_no,
    ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY seq_no DESC, id) AS seq_no_2,
    AVG(discount_pct_int) OVER (PARTITION BY channel) AS channel_avg_discount_pct_int,
    SUM(cumulative_discount_pct_int) OVER (PARTITION BY channel ORDER BY ends_on_period, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_cumulative_discount_pct_int
FROM metrics;
