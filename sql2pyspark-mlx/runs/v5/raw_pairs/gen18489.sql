WITH cleaned AS (
    SELECT
        id,
        full_name,
        email,
        manager_id,
        job_title,
        salary,
        is_remote,
        INITCAP(TRIM(job_title)) AS job_title_title
    FROM employees
    WHERE job_title IS NOT NULL
)
SELECT
    manager_id,
    SORT_ARRAY(COLLECT_SET(job_title)) AS job_title_list,
    COUNT(DISTINCT id) AS distinct_ids
FROM cleaned
GROUP BY manager_id
HAVING distinct_ids < 1000
ORDER BY manager_id DESC NULLS LAST;
