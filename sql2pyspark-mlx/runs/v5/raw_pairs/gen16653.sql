WITH kept_keys AS (
    SELECT level
    FROM employees
    INTERSECT ALL
    SELECT level
    FROM employees
    WHERE level IN ('L2', 'L4')
),
reconciled AS (
    SELECT s.*
    FROM employees s
    INNER JOIN kept_keys k ON s.level = k.level
),
scored AS (
    SELECT
        *,
        MIN(salary) OVER (PARTITION BY dept_id) AS dept_id_min_salary,
        COUNT(salary) OVER (PARTITION BY manager_id) AS manager_id_count_salary,
        ROUND(salary / SUM(salary) OVER (PARTITION BY job_title), 4) AS salary_share
    FROM reconciled
    WHERE email LIKE '%@startup.io'
),
with_windows AS (
    SELECT
        id,
        full_name,
        email,
        manager_id,
        is_remote,
        level,
        dept_id_min_salary,
        manager_id_count_salary,
        salary_share,
        AVG(salary) OVER w_grp AS dept_id_avg_salary
    FROM scored
    WINDOW w_grp AS (PARTITION BY dept_id)
),
valid AS (
    SELECT
        *,
        manager_id_count_salary / NULLIF(salary_share, 0) AS manager_id_count_salary_per_salary_share,
        COALESCE(level, 'OTHER') AS resolved_level
    FROM with_windows
    WHERE level IS NOT NULL
      AND manager_id_count_salary BETWEEN 8 AND 16
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER w_recent AS manager_id_count_salary_per_salary_share_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY manager_id, level ORDER BY manager_id_count_salary_per_salary_share DESC NULLS LAST) AS manager_id_count_salary_per_salary_share_percent_rank
    FROM valid
    WINDOW w_recent AS (PARTITION BY level ORDER BY manager_id_count_salary_per_salary_share DESC NULLS FIRST)
),
joined AS (
    SELECT
        a.*,
        e.level AS employee_level,
        e.full_name AS employee_full_name,
        e.salary
    FROM enriched a
    LEFT JOIN employees e
        ON a.manager_id = e.id
),
base AS (
    SELECT
        *,
        COALESCE(employee_full_name, 'N/A') AS employee_full_name_filled,
        COALESCE(full_name, 'unassigned') AS full_name_filled
    FROM joined
    WHERE salary_share <= 0.35
)
SELECT
    manager_id,
    metric,
    val
FROM (
    SELECT
        manager_id,
        manager_id_count_salary_per_salary_share_percent_rank,
        salary
    FROM base
) src
UNPIVOT INCLUDE NULLS (
    val FOR metric IN (manager_id_count_salary_per_salary_share_percent_rank, salary)
);
