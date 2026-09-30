WITH deduped AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY hire_date DESC, id ASC) AS rank_in_group
        FROM employees
    ) ranked
    WHERE rank_in_group = 1
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        NTILE(5) OVER (ORDER BY salary ASC, id ASC) AS salary_bucket,
        AVG(salary) OVER (PARTITION BY dept_id ORDER BY salary ASC, id ASC ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS salary_avg_3_prior,
        COUNT(salary) OVER (PARTITION BY dept_id) AS dept_id_count_salary
    FROM deduped
),
deduped_employees AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY level ORDER BY dept_id_count_salary DESC, id ASC) AS dedup_rank
    FROM with_windows
    QUALIFY dedup_rank = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        full_name,
        email,
        dept_id,
        manager_id,
        job_title,
        salary,
        hire_date,
        is_remote,
        level,
        rank_in_group,
        salary_bucket,
        salary_avg_3_prior,
        dept_id_count_salary,
        dedup_rank,
        FIRST_VALUE(level) IGNORE NULLS OVER (PARTITION BY job_title ORDER BY dedup_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_level,
        ROW_NUMBER() OVER w_recent AS seq_no,
        LAST_VALUE(salary_bucket, TRUE) OVER (PARTITION BY job_title, level ORDER BY salary_bucket ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_salary_bucket
    FROM deduped_employees
    WINDOW w_recent AS (PARTITION BY dept_id ORDER BY dept_id_count_salary ASC, id ASC)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        dept_id,
        job_title,
        salary,
        hire_date,
        level,
        rank_in_group,
        salary_bucket,
        dedup_rank,
        first_known_level,
        last_known_salary_bucket,
        FLOOR(rank_in_group) AS rank_in_group_whole,
        LEAST(dedup_rank, salary_bucket) AS least_dedup_rank_salary_bucket
    FROM scored
    WHERE hire_date > TO_DATE('2023-09-09')
      AND manager_id IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        ROUND(last_known_salary_bucket / SUM(last_known_salary_bucket) OVER (PARTITION BY dept_id), 4) AS last_known_salary_bucket_share,
        RANK() OVER (PARTITION BY job_title ORDER BY rank_in_group_whole ASC NULLS FIRST) AS rank_in_group_whole_rank
    FROM cleaned
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        dept_id,
        job_title,
        salary,
        hire_date,
        level,
        rank_in_group,
        salary_bucket,
        dedup_rank,
        first_known_level,
        last_known_salary_bucket,
        rank_in_group_whole,
        least_dedup_rank_salary_bucket,
        last_known_salary_bucket_share,
        rank_in_group_whole_rank,
        LEAD(rank_in_group_whole_rank, 1, 0) OVER (PARTITION BY job_title ORDER BY hire_date ASC, id ASC) AS next_rank_in_group_whole_rank,
        SUM(rank_in_group_whole_rank) OVER w_grp AS cumulative_rank_in_group_whole_rank,
        NTILE(10) OVER (PARTITION BY level, job_title ORDER BY salary_bucket DESC, id ASC) AS salary_bucket_bucket,
        ROUND(rank_in_group_whole_rank / SUM(rank_in_group_whole_rank) OVER (PARTITION BY dept_id), 4) AS rank_in_group_whole_rank_share,
        LEAD(least_dedup_rank_salary_bucket, 2) OVER w_grp_2 AS next_least_dedup_rank_salary_bucket
    FROM metrics
    WINDOW w_grp AS (PARTITION BY level ORDER BY last_known_salary_bucket_share ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_grp_2 AS (PARTITION BY level ORDER BY last_known_salary_bucket ASC, id ASC)
)
SELECT
    s.id,
    s.dept_id,
    s.job_title,
    s.hire_date,
    s.level,
    s.salary_bucket,
    s.dedup_rank,
    s.last_known_salary_bucket,
    s.rank_in_group_whole,
    CASE WHEN job_title RLIKE '^(Designer|Analyst)$' THEN 'CORE' WHEN job_title RLIKE '^(Engineer|Manager)$' THEN 'PRIORITY' ELSE 'OTHER_ZONE' END AS job_title_zone,
    (SELECT MIN(t2.salary) FROM employees t2 WHERE t2.dept_id = s.dept_id) AS min_employee_salary
FROM enriched s
ORDER BY rank_in_group_whole DESC NULLS FIRST;
