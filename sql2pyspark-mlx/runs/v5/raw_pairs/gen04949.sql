with lines as (
    select
        id,
        customer_id,
        code
    from carts
    lateral view explode(coupon_codes) li as code
)
select
    customer_id,
    code,
    count(*) filter (where code = 'SPRING10') as spring10_rows,
    size(collect_set(code)) as num_codes,
    count(case when code = 'WELCOME5' then 1 end) as welcome5_count,
    count(distinct customer_id) as distinct_customer_ids,
    count(*) as num_rows,
    count(case when code = 'WELCOME5' then 1 end) as welcome5_count_2
from lines
group by customer_id, code
order by spring10_rows nulls last;
