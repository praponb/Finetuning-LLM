WITH flattened AS (
    SELECT
        id,
        service,
        level,
        key,
        attr_value
    FROM app_logs
    LATERAL VIEW EXPLODE(context) x AS key, attr_value
)
SELECT
    id,
    service,
    key,
    attr_value
FROM flattened
ORDER BY service, id;
