WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY opened_date, id) AS rn
    FROM accounts
    QUALIFY rn = 1
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN balance >= 73000 THEN 'LARGE' ELSE 'SMALL' END AS balance_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(balance, ARRAY(0.25, 0.5, 0.75)) AS balance_quartiles
FROM deduped
GROUP BY CASE WHEN balance >= 73000 THEN 'LARGE' ELSE 'SMALL' END
HAVING COUNT(DISTINCT id) > 1
ORDER BY distinct_ids NULLS FIRST
