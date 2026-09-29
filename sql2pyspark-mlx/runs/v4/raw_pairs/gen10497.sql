WITH deduped AS (
    SELECT *
    FROM departments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY location, dept_name ORDER BY budget DESC, id) = 1
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER w_part AS budget_percent_rank,
        ROW_NUMBER() OVER (ORDER BY budget, id) AS seq_no
    FROM deduped
    WINDOW w_part AS (PARTITION BY dept_name, location ORDER BY budget NULLS LAST)
),
enriched AS (
    SELECT
        *,
        SUM(budget_percent_rank) OVER (ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_budget_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget_percent_rank DESC, id) AS seq_no_2
    FROM scored
)
SELECT
    dept_name,
    CASE WHEN seq_no_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END AS seq_no_2_tier,
    SUM(CASE WHEN location = 'Bangkok' THEN seq_no ELSE 0 END) AS bangkok_seq_no,
    COUNT_IF(dept_name = 'HR') AS hr_events,
    SORT_ARRAY(COLLECT_SET(dept_name)) AS dept_name_list,
    COUNT(CASE WHEN dept_name = 'HR' THEN 1 END) AS hr_count,
    SIZE(COLLECT_SET(dept_name)) AS num_dept_names
FROM enriched
GROUP BY dept_name, CASE WHEN seq_no_2 >= 7 THEN 'LARGE' ELSE 'SMALL' END
HAVING bangkok_seq_no < 0
