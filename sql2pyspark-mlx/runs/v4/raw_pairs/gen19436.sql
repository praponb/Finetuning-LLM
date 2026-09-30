WITH valid AS (
    SELECT
        *,
        UPPER(location) AS location_upper,
        COALESCE(location, 'N/A') AS location_filled
    FROM departments
    WHERE dept_name IN ('Sales', 'Engineering', 'Legal')
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY budget DESC NULLS LAST, id) AS rn
        FROM valid
    ) ranked
    WHERE rn = 1
),
ranked AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY location ORDER BY rn DESC, id) AS latest_rank
        FROM latest
    ) t
    WHERE latest_rank = 1
),
matched_keys AS (
    SELECT location, id
    FROM ranked
    EXCEPT
    SELECT location, id
    FROM departments
    WHERE budget BETWEEN 1131000 AND 1179000
),
reconciled AS (
    SELECT s.*
    FROM ranked s
    JOIN matched_keys k ON s.location = k.location AND s.id = k.id
)
SELECT
    id,
    dept_name,
    location,
    budget,
    location_filled,
    rn
FROM reconciled
ORDER BY id DESC;
