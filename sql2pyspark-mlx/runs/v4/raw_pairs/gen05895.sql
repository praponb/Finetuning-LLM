WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(budget, 0) budget_filled,
        INITCAP(TRIM(dept_name)) dept_name_title
    FROM departments
    WHERE location IS NOT NULL
),
cleaned_departments AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN location IN ('Berlin', 'Tokyo') THEN 'Priority' ELSE 'Other' END location_group
    FROM cleaned
    WHERE budget >= 1566000
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget_filled DESC NULLS LAST, id) AS dedup_rank
        FROM cleaned_departments
    ) t
    WHERE dedup_rank = 1
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM deduped s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees ch
            WHERE ch.dept_id = s.id
              AND ch.manager_id IN (10, 14, 17, 31)
        )
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_name,
        location,
        budget,
        budget_filled,
        dept_name_title,
        location_group,
        CAST(dedup_rank AS DOUBLE) dedup_rank_num,
        COALESCE(dept_name, 'UNKNOWN') resolved_dept_name
    FROM eligible
    WHERE location IS NOT NULL
)
SELECT
    id,
    location,
    budget_filled,
    dept_name_title,
    location_group,
    dedup_rank_num,
    resolved_dept_name,
    CASE WHEN location RLIKE '^(New York|Chiang Mai)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS location_zone
FROM valid
ORDER BY dedup_rank_num DESC, location_zone;
