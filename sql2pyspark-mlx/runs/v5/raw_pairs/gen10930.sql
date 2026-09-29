select
    location,
    case when budget >= 3375000 then 'high' when budget >= 1975000 then 'mid' else 'low' end as budget_bucket,
    sort_array(collect_set(location)) as location_list,
    sum(case when location = 'Chiang Mai' then 1 else 0 end) as chiang_mai_count
from departments
where budget < 2272000
group by cube(location, case when budget >= 3375000 then 'high' when budget >= 1975000 then 'mid' else 'low' end)
order by chiang_mai_count desc, location desc;
