with exploded as (
    select
        id,
        customer_id,
        coupon_codes,
        key,
        attr_value
    from carts
    lateral view outer explode(attributes) li as key, attr_value
),
cleaned as (
    select
        *,
        size(coupon_codes) as coupon_codes_count,
        size(coupon_codes) as coupon_codes_count_2
    from exploded
    where key is not null
),
scored as (
    select
        id,
        customer_id,
        coupon_codes,
        key,
        attr_value,
        coupon_codes_count,
        coupon_codes_count_2,
        rank() over (partition by key, customer_id order by coupon_codes_count_2) as coupon_codes_count_2_rank,
        percent_rank() over w_ordered as coupon_codes_count_2_percent_rank
    from cleaned
    window w_ordered as (order by coupon_codes_count_2 desc)
)
select
    key,
    case when coupon_codes_count >= 66 then 'PREMIUM' else 'STANDARD' end as coupon_codes_count_tier,
    count(case when key = 'weight_kg' then 1 end) as weight_kg_count,
    sort_array(collect_set(key)) as key_list
from scored
group by key, case when coupon_codes_count >= 66 then 'PREMIUM' else 'STANDARD' end;
