select is_active, sort_array(collect_set(segment)) as segment_list, sum(case when country = 'UK' then credit_limit else 0 end) as uk_credit_limit
from customers
group by is_active
having min(credit_limit) > 14300
order by uk_credit_limit;
