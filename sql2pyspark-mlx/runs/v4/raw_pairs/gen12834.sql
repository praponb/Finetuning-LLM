WITH metrics AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        ROW_NUMBER() OVER w_recent AS row_seq,
        NTILE(4) OVER w_recent AS budget_bucket,
        ROUND(budget / SUM(budget) OVER (PARTITION BY location), 4) AS budget_share
    FROM departments
    WINDOW w_recent AS (PARTITION BY dept_name ORDER BY budget DESC, id ASC)
)
SELECT
    id,
    dept_name,
    location,
    budget,
    row_seq,
    budget_bucket,
    budget_share,
    CASE WHEN location RLIKE '^(Singapore|New York)$' THEN 'EXTENDED' WHEN location RLIKE '^(Bangkok|Chiang Mai)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS location_zone
FROM metrics;
