with valid as (
    select *, round(credit_limit * 0.9, 2) as credit_limit_scaled
    from customers
    where city is not null
),
latest_match as (
    select
        s.*,
        top1.order_discount,
        top1.order_status
    from valid s
    left join lateral (
            select c.discount as order_discount, c.status as order_status
            from orders c
            where c.customer_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) top1 on true
),
scored as (
    select
        *,
        avg(order_discount) over (order by signup_date, id rows between 2 preceding and 1 preceding) as order_discount_avg_2_prior,
        dense_rank() over (order by credit_limit nulls first) as credit_limit_dense_rank
    from latest_match
)
select
    segment,
    count(*) as num_rows,
    avg(order_discount_avg_2_prior) filter (where segment = 'Corporate') as corporate_avg_order_discount_avg_2_prior,
    count(*) filter (where segment = 'SMB') as smb_rows,
    round(sum(order_discount) / nullif(count(case when country = 'UK' then 1 end), 0), 2) as order_discount_per_uk,
    percentile_approx(order_discount_avg_2_prior, 0.9) as order_discount_avg_2_prior_p90,
    sort_array(collect_set(city)) as city_list
from scored
group by segment
order by order_discount_per_uk desc nulls first
