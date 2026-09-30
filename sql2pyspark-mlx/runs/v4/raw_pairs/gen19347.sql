SELECT COUNT(CASE WHEN location = 'Chiang Mai' THEN 1 END) AS chiang_mai_count
FROM departments
ORDER BY chiang_mai_count ASC;
