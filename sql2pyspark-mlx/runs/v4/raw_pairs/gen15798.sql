select
    customer_id,
    case when balance >= 84000 then 'HIGH' else 'LOW' end as balance_tier,
    round(sum(balance) / nullif(count(case when status = 'FROZEN' then 1 end), 0), 2) as balance_per_frozen,
    round(avg(balance), 2) as avg_balance,
    approx_percentile(balance, array(0.25, 0.5, 0.75)) as balance_quartiles
from accounts
group by customer_id, case when balance >= 84000 then 'HIGH' else 'LOW' end
order by customer_id asc nulls first;
