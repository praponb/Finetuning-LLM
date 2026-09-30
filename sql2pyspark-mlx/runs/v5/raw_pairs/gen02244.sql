select city, concat_ws(', ', sort_array(collect_set(country))) as country_values from suppliers group by city order by country_values desc, city;
