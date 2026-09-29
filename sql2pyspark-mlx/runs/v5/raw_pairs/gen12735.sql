SELECT segment, SORT_ARRAY(COLLECT_SET(country)) country_list, AVG(age) avg_age, MAX(age) max_age FROM customers WHERE email IS NULL GROUP BY segment HAVING max_age < 67 ORDER BY avg_age
