WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        service,
        SIZE(context) AS num_context,
        context['version'] AS version
    FROM app_logs
)
-- Ranking and gap features
SELECT
    *,
    COUNT(num_context) OVER (PARTITION BY service) AS service_count_num_context,
    CUME_DIST() OVER (PARTITION BY service ORDER BY num_context DESC NULLS LAST) AS num_context_cume_dist
FROM parsed;
