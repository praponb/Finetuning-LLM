select
    country,
    city,
    case when rating >= 3.7 then 'high' when rating >= 1.5 then 'mid' else 'low' end as rating_bucket,
    sort_array(collect_set(country)) as country_list,
    round(avg(rating), 2) as avg_rating,
    sum(case when city = 'Berlin' then 1 else 0 end) as berlin_count
from suppliers
where supplier_name is not null and country not in ('UK', 'TH', 'US')
group by 1, 2, 3
