select
    case when country in ('TH', 'SG') then 'core' else 'other' end as country_group,
    min(age) as min_age,
    sort_array(collect_set(country)) as country_list,
    round(avg(age), 2) as avg_age
from customers
group by 1
