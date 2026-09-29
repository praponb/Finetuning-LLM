select *
from (
    select
        country,
        segment,
        age
    from customers
) src
pivot (
    max(age) as value_sum,
    count(1) as item_count
    for segment in ('Corporate', 'SMB')
)
