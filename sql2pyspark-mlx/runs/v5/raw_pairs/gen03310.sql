SELECT COUNT(CASE WHEN location = 'London' THEN 1 END) london_count FROM departments ORDER BY london_count ASC
