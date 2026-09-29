select max(sqft) as max_sqft, sort_array(collect_set(region)) as region_list, count(distinct sqft) as unique_citys from stores where sqft = 7677 or city in ('Chiang Mai', 'Berlin')
