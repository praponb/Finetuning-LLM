select city, concat_ws(', ', sort_array(collect_set(city))) as city_values from stores group by city order by city_values, city limit 3
