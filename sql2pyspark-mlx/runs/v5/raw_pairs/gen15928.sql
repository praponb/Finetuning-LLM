WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        email,
        dept_id,
        job_title,
        salary,
        hire_date,
        level,
        SUBSTRING(full_name, 1, 4) AS full_name_prefix,
        COALESCE(level, 'UNKNOWN') AS level_clean
    FROM employees
    WHERE hire_date >= TO_DATE('2023-08-17')
      AND level IS DISTINCT FROM 'L2'
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC, id) AS latest_rank
        FROM base
    ) ranked
    WHERE latest_rank = 1
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        dept_id,
        job_title,
        hire_date,
        full_name_prefix,
        latest_rank,
        SUBSTRING(job_title, 1, 4) AS job_title_prefix
    FROM ranked
    WHERE job_title IS DISTINCT FROM 'Designer'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *, RANK() OVER (PARTITION BY dept_id ORDER BY hire_date DESC) AS rank_in_group
    FROM cleaned
    QUALIFY rank_in_group = 1
),
matched_keys AS (
    -- Set intersection on keys
    SELECT id, dept_id
    FROM latest
    INTERSECT ALL
    SELECT id, dept_id
    FROM employees
    WHERE salary <= 60000
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM latest s
    INNER JOIN matched_keys k ON s.id = k.id AND s.dept_id = k.dept_id
),
joined AS (
    -- Attach departments attributes
    SELECT
        e.dept_id,
        e.hire_date,
        e.full_name_prefix,
        e.job_title_prefix,
        d.location
    FROM verified e
    LEFT JOIN departments d
        ON e.dept_id = d.id
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        LAST_DAY(hire_date) AS hire_date_month_end,
        COALESCE(location, 'DIRECT') AS location_filled
    FROM joined
    WHERE hire_date > DATE '2023-07-20'
      AND dept_id IN (3, 8, 31, 33)
)
SELECT
    location,
    hire_date_month_end,
    location_filled,
    CASE WHEN location RLIKE '^(Chiang Mai|Berlin)$' THEN 'GROUP_B' WHEN location RLIKE '^(Bangkok|New York)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS location_zone
FROM valid;
