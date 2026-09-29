WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY ends_on DESC, id) AS dedup_rank
    FROM promotions
    QUALIFY dedup_rank = 1
)
SELECT
    promo_code,
    PERCENTILE_APPROX(discount_pct, ARRAY(0.25, 0.5, 0.75)) AS discount_pct_quartiles,
    COUNT_IF(channel = 'store') AS store_events,
    ROUND(SUM(discount_pct) / NULLIF(COUNT(CASE WHEN channel = 'store' THEN 1 END), 0), 2) AS discount_pct_per_store
FROM deduped
GROUP BY promo_code;
