select city, sort_array(collect_set(country)) country_list from suppliers group by 1;
