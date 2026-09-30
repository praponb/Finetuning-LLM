SELECT MONTH(signup_date) AS signup_month, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(segment))) AS segment_values
FROM customers
GROUP BY signup_month;
