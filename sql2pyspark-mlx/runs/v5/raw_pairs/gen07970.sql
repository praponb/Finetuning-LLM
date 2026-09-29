select concat_ws(', ', sort_array(collect_set(city))) city_values, count(rating) rating_count from suppliers order by city_values, rating_count desc
