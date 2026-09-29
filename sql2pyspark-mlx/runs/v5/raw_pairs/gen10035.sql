select
    segment_group,
    credit_limit_bucket,
    stddev_age,
    min_age
from (
    select
        case when segment in ('Consumer', 'Corporate') then 'priority' else 'other' end as segment_group,
        case when credit_limit >= 17600 then 'high' when credit_limit >= 6200 then 'mid' else 'low' end as credit_limit_bucket,
        round(stddev(age), 2) as stddev_age,
        min(age) as min_age
    from customers
    group by case when segment in ('Consumer', 'Corporate') then 'priority' else 'other' end, case when credit_limit >= 17600 then 'high' when credit_limit >= 6200 then 'mid' else 'low' end
) as s
where min_age < 52
order by segment_group;
