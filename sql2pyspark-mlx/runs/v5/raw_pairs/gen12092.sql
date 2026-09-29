WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        service,
        host,
        k,
        v
    FROM app_logs
    LATERAL VIEW OUTER EXPLODE(context) li AS k, v
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN host = 'h3' THEN 1 ELSE 0 END is_h3
    FROM flattened
    WHERE k IS NOT NULL
      AND service IN ('auth', 'billing')
),
enriched AS (
    -- Running and moving aggregates
    SELECT *, PERCENT_RANK() OVER w_part is_h3_percent_rank
    FROM cleaned
    WINDOW w_part AS (ORDER BY is_h3 DESC NULLS LAST)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT
    SELECT id
    FROM app_logs
    WHERE host = 'h4'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN kept_keys k ON s.id = k.id
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        is_h3_percent_rank / NULLIF(is_h3, 0) is_h3_percent_rank_per_is_h3,
        CONCAT_WS('-', service, host) service_host_key
    FROM reconciled
    WHERE k = 'region'
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        service,
        host,
        v,
        is_h3_percent_rank,
        is_h3_percent_rank_per_is_h3,
        RANK() OVER (PARTITION BY service, host ORDER BY is_h3_percent_rank_per_is_h3) is_h3_percent_rank_per_is_h3_rank,
        DENSE_RANK() OVER (PARTITION BY host ORDER BY is_h3_percent_rank_per_is_h3 DESC) is_h3_percent_rank_per_is_h3_dense_rank,
        COUNT(is_h3_percent_rank_per_is_h3) OVER (PARTITION BY service) service_count_is_h3_percent_rank_per_is_h3,
        RANK() OVER (ORDER BY is_h3_percent_rank DESC NULLS LAST) is_h3_percent_rank_rank,
        RANK() OVER (PARTITION BY host ORDER BY is_h3 DESC NULLS LAST) is_h3_rank
    FROM filtered
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        service,
        is_h3_percent_rank,
        is_h3_percent_rank_per_is_h3
    FROM with_windows
) src
UNPIVOT INCLUDE NULLS (
    val FOR metric IN (is_h3_percent_rank AS IS_H3_PERCENT_RANK_VAL, is_h3_percent_rank_per_is_h3 AS IS_H3_PERCENT_RANK_PER_IS_H3)
)
