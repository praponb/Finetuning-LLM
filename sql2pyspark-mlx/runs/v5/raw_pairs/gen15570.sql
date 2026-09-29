select
    case when location in ('Tokyo', 'Berlin') then 'tier_1' else 'other' end location_group,
    dept_name,
    sum(case when location = 'Berlin' then 1 else 0 end) berlin_count,
    min(budget) min_budget
from departments
group by 1, 2
having count(distinct budget) >= 4
order by berlin_count desc;
