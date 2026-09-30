with exploded as (
    select
        id,
        customer_id,
        explode(coupon_codes) as code
    from carts
)
select
    code,
    size(collect_set(code)) as num_codes,
    sort_array(collect_set(code)) as code_list,
    sort_array(collect_set(code)) as code_list_2
from exploded
group by code
order by code;
