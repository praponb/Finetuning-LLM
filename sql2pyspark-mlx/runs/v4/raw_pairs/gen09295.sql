select
    case when credit_limit >= 17800 then 'high' when credit_limit >= 6200 then 'mid' else 'low' end as credit_limit_bucket,
    sum(credit_limit) as total_credit_limit,
    round(stddev(credit_limit), 2) as stddev_credit_limit,
    count(*) as num_customers
from customers
where segment not in ('SMB', 'Consumer', 'Corporate')
group by case when credit_limit >= 17800 then 'high' when credit_limit >= 6200 then 'mid' else 'low' end
order by stddev_credit_limit;
