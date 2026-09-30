select year(signup_date) as signup_year, concat_ws(', ', sort_array(collect_set(segment))) as segment_values, count(*) as cnt from customers group by year(signup_date) having count(*) >= 4;
