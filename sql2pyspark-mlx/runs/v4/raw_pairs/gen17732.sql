WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT(location, ' - ', dept_name) AS location_dept_name,
        ROUND(budget, 2) AS budget_rounded
    FROM departments
    WHERE budget BETWEEN 1310000 AND 1488000
),
remaining_keys AS (
    -- Set difference on keys
    SELECT dept_name, id
    FROM valid
    EXCEPT DISTINCT
    SELECT dept_name, id
    FROM departments
    WHERE dept_name = 'HR'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    INNER JOIN remaining_keys k ON s.dept_name = k.dept_name AND s.id = k.id
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY dept_name ORDER BY budget_rounded DESC NULLS LAST) AS budget_rounded_dense_rank,
        STDDEV_SAMP(budget) OVER w_ordered AS location_stddev_samp_budget,
        CUME_DIST() OVER (PARTITION BY dept_name ORDER BY budget_rounded NULLS LAST) AS budget_rounded_cume_dist,
        SUM(budget) OVER (PARTITION BY dept_name) AS dept_name_sum_budget,
        MIN(budget_rounded) OVER w_ordered AS location_min_budget_rounded
    FROM reconciled
    WINDOW w_ordered AS (PARTITION BY location)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        location,
        dept_name,
        location_min_budget_rounded
    FROM enriched
) src
PIVOT (
    MAX(location_min_budget_rounded) AS amount
    FOR dept_name IN ('Finance', 'Sales', 'Engineering')
);
