WITH metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(discount_pct, 1) OVER (PARTITION BY channel ORDER BY discount_pct, id) AS next_discount_pct,
        LEAD(discount_pct, 2) OVER (PARTITION BY promo_code ORDER BY starts_on, id) AS next_discount_pct_2,
        SUM(discount_pct) OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct,
        ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY ends_on DESC, id) AS row_seq,
        FIRST_VALUE(channel) IGNORE NULLS OVER w_running AS first_known_channel
    FROM promotions
    WINDOW w_running AS (PARTITION BY promo_code, channel ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    next_discount_pct_2,
    cumulative_discount_pct,
    first_known_channel,
    CASE WHEN promo_code RLIKE '^(BUNDLE|WELCOME5)$' THEN 'CORE' WHEN promo_code RLIKE '^(SPRING10|VIP)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS promo_code_zone
FROM metrics;
