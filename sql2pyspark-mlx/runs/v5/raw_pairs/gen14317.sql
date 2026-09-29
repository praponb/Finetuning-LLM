select
    case when region in ('Central', 'South') then 'group_a' else 'other' end as region_group,
    region,
    max(sqft) as max_sqft,
    sum(case when city = 'New York' then sqft else 0 end) as new_york_sqft,
    sum(case when region = 'East' then sqft else 0 end) as east_sqft
from stores
where region = 'East'
group by region_group, region
having max(sqft) < 11200;
