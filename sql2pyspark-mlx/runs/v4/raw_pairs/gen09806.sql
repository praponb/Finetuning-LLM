select region, new_york_count, latest_opened_date
from (
    select region, count(case when city = 'New York' then 1 end) as new_york_count, max(opened_date) as latest_opened_date
    from stores
    group by region
) as agg
where new_york_count < 8;
