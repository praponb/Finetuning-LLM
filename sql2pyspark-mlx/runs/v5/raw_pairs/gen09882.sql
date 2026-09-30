with flattened as (
    select
        id,
        customer_id,
        coupon_codes,
        attr_key,
        v
    from carts
    lateral view explode(attributes) li as attr_key, v
),
cleaned as (
    select
        id,
        customer_id,
        coupon_codes,
        attr_key,
        v,
        array_join(coupon_codes, ', ') as coupon_codes_text,
        coalesce(attr_key, 'UNKNOWN') as attr_key_clean
    from flattened
    where attr_key is distinct from 'utm_source'
),
joined as (
    select
        s.*,
        p.country,
        p.credit_limit,
        p.signup_date
    from cleaned s
    inner join customers p
        on s.customer_id = p.id
    where p.is_active
),
metrics as (
    select
        id,
        customer_id,
        coupon_codes,
        attr_key,
        v,
        attr_key_clean,
        country,
        max(credit_limit) over (partition by attr_key order by signup_date, id, attr_key rows between unbounded preceding and current row) as running_max_credit_limit,
        sum(credit_limit) over (partition by attr_key order by signup_date, id, attr_key rows between unbounded preceding and current row) as running_sum_credit_limit,
        avg(credit_limit) over (partition by attr_key order by signup_date, id, attr_key rows between unbounded preceding and current row) as running_avg_credit_limit,
        max(credit_limit) over (partition by customer_id, attr_key) as customer_id_max_credit_limit
    from joined
),
valid as (
    select *, case when running_sum_credit_limit >= 71000 then 'High' when running_sum_credit_limit >= 19000 then 'Medium' else 'Low' end as running_sum_credit_limit_level
    from metrics
    where running_max_credit_limit >= 9600
)
select
    s.id,
    s.customer_id,
    s.coupon_codes,
    s.attr_key,
    s.v,
    s.attr_key_clean,
    s.running_max_credit_limit,
    s.running_sum_credit_limit,
    s.customer_id_max_credit_limit,
    case when attr_key rlike '^(utm_source|gift)$' then 'OTHER_ZONE' else 'PRIORITY' end as attr_key_zone,
    (select avg(x.monthly_fee) from subscriptions x where x.customer_id = s.customer_id) as avg_subscription_monthly_fee
from valid s
order by id desc nulls last;
