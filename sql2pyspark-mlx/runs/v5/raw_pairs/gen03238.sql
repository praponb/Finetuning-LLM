select method_group, unique_methods
from (
    select case when method in ('cash', 'paypal') then 'group_a' else 'other' end as method_group, count(distinct amount) as unique_methods
    from payments
    where method = 'bank_transfer'
    group by case when method in ('cash', 'paypal') then 'group_a' else 'other' end
) as t
where unique_methods > 10;
