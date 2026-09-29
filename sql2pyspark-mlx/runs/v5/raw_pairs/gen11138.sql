WITH src AS (
    SELECT *
    FROM subscriptions
    WHERE status = 'PAUSED'
),
status_group_summary AS (
    SELECT CASE WHEN status IN ('ACTIVE', 'EXPIRED') THEN 'group_a' ELSE 'other' END AS status_group, SORT_ARRAY(COLLECT_SET(status)) AS status_list, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(plan))) AS plan_values
    FROM src
    GROUP BY CASE WHEN status IN ('ACTIVE', 'EXPIRED') THEN 'group_a' ELSE 'other' END
)
SELECT *
FROM status_group_summary;
