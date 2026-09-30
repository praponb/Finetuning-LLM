WITH filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CONCAT_WS(',', dept_name, location) dept_name_location_key,
        CAST(budget AS INT) budget_int,
        COALESCE(location, 'OTHER') location_clean
    FROM departments
    WHERE dept_name IS NOT NULL
      AND dept_name IN ('Finance', 'Legal', 'HR')
)
-- QUALIFY-style window filter
SELECT *
FROM filtered
QUALIFY ROW_NUMBER() OVER (PARTITION BY dept_name, location ORDER BY budget DESC, id ASC) = 1;
