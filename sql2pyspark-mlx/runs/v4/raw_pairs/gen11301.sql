WITH valid AS (
    -- Basic cleansing and defaults
    SELECT *, TRIM(segment) AS clean_segment
    FROM customers
    WHERE city IS NOT NULL
      AND city = 'Tokyo'
)
SELECT segment, city
FROM valid
INTERSECT
SELECT segment, city
FROM customers
WHERE LOWER(TRIM(name)) = 'olivia garcia';
