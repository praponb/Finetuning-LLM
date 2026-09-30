WITH lines AS (
    SELECT
        id,
        effective_from,
        EXPLODE(discounts) AS val
    FROM price_tiers
)
SELECT
    CASE WHEN val >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(val, 0.9) AS val_p90,
    COUNT(*) AS cnt,
    COUNT(*) AS num_rows
FROM lines
GROUP BY CASE WHEN val >= 64 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY cnt DESC
