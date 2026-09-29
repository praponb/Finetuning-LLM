SELECT MIN(credit_limit) AS min_credit_limit, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, ROUND(STDDEV(credit_limit), 2) AS stddev_credit_limit FROM customers;
