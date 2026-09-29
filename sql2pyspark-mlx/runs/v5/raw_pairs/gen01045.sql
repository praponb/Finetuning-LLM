with filtered as (
    -- Keep recent, valid rows
    select
        id,
        country,
        city,
        segment,
        signup_date,
        is_active,
        split(lower(email), '@')[0] as email_user,
        coalesce(segment, 'OTHER') as segment_filled
    from customers
    where not is_active
),
ranked as (
    -- Keep one row per key (latest first)
    select *, rank() over (partition by city order by signup_date desc) as row_num
    from filtered
    qualify row_num = 1
),
not_flagged as (
    -- Anti-join via a correlated subquery
    select s.*
    from ranked s
    where not exists (
            select 1
            from orders c
            where c.customer_id = s.id
              and c.order_date < s.signup_date
        )
),
with_last as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.id,
        s.city,
        s.segment,
        s.email_user,
        s.segment_filled,
        s.row_num,
        top1.amount
    from not_flagged s
    left join lateral (
            select c.amount
            from orders c
            where c.customer_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) top1 on true
),
scored as (
    -- Window metrics over several different partitions
    select
        id,
        city,
        email_user,
        segment_filled,
        percent_rank() over w_hist as amount_percent_rank,
        dense_rank() over (order by amount nulls last) as amount_dense_rank,
        lead(amount, 1) over w_ordered as next_amount
    from with_last
    window w_hist as (partition by city, segment order by amount desc), w_ordered as (order by amount, id)
)
select
    id,
    email_user,
    segment_filled,
    amount_dense_rank,
    next_amount
from scored
