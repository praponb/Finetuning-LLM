WITH cleaned AS (
    SELECT
        *,
        CASE WHEN dept_name IN ('Sales', 'Finance') THEN 'Priority' ELSE 'Other' END dept_name_group,
        COALESCE(location, 'NONE') location_clean
    FROM departments
    WHERE dept_name IS NOT NULL
      AND budget < 2671000
),
enriched AS (
    SELECT
        *,
        FIRST_VALUE(location, TRUE) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_location,
        ROW_NUMBER() OVER w_part row_seq,
        LAST_VALUE(location, TRUE) OVER (PARTITION BY dept_name, location ORDER BY budget, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_location,
        ROW_NUMBER() OVER (PARTITION BY dept_name ORDER BY budget, id) row_seq_2
    FROM cleaned
    WINDOW w_part AS (PARTITION BY location ORDER BY budget, id)
),
eligible AS (
    SELECT s.*
    FROM enriched s
    WHERE NOT EXISTS (
            SELECT 1
            FROM employees c
            WHERE c.dept_id = s.id
              AND UPPER(TRIM(c.full_name)) = 'HENRY FORD'
        )
),
deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY row_seq DESC NULLS FIRST, id) AS dedup_rank
        FROM eligible
    ) ranked
    WHERE dedup_rank = 1
),
enriched_departments AS (
    SELECT
        id,
        dept_name,
        location,
        budget,
        dept_name_group,
        location_clean,
        first_known_location,
        row_seq,
        last_known_location,
        row_seq_2,
        dedup_rank,
        AVG(row_seq_2) OVER (PARTITION BY dept_name ORDER BY dedup_rank, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) row_seq_2_avg_2_prior,
        AVG(row_seq) OVER (PARTITION BY location ORDER BY budget, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) row_seq_avg_2_prior,
        LAST_VALUE(row_seq, TRUE) OVER (PARTITION BY dept_name ORDER BY row_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_row_seq
    FROM deduped
),
with_last AS (
    SELECT s.*, x.level
    FROM enriched_departments s
    LEFT JOIN LATERAL (
            SELECT c.level
            FROM employees c
            WHERE c.dept_id = s.id
            ORDER BY c.salary DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    dept_name,
    location,
    budget,
    dept_name_group,
    first_known_location,
    row_seq_2,
    dedup_rank,
    row_seq_2_avg_2_prior
FROM with_last
ORDER BY location, dept_name DESC
