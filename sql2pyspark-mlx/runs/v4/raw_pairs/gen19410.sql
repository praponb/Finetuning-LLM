select
    case when rating >= 4.1 then 'high' when rating >= 1.8 then 'mid' else 'low' end rating_bucket,
    concat_ws(', ', sort_array(collect_set(city))) city_values,
    sum(case when city = 'Berlin' then 1 else 0 end) berlin_count,
    sort_array(collect_set(country)) country_list
from suppliers
where supplier_name like '%Suppl%'
group by case when rating >= 4.1 then 'high' when rating >= 1.8 then 'mid' else 'low' end
having berlin_count > 42
order by berlin_count, city_values, rating_bucket
limit 3;
