SELECT YEAR(paid_at) paid_year, SORT_ARRAY(COLLECT_SET(method)) method_list FROM payments GROUP BY YEAR(paid_at) ORDER BY paid_year DESC;
