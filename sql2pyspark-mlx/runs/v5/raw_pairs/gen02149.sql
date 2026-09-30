SELECT country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) country_values, SUM(rating) total_rating FROM suppliers GROUP BY country ORDER BY country DESC
