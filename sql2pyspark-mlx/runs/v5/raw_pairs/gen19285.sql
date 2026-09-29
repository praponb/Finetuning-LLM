SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_channel,
    ROW_NUMBER() OVER (PARTITION BY channel ORDER BY ends_on DESC, id) event_seq,
    SUM(discount_pct) OVER (ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_discount_pct
FROM promotions;
