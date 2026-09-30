with exploded as (
    -- Unnest the array into one row per element
    select
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    from carts
    lateral view outer explode(attributes) t as k, v
),
enriched as (
    -- Attach customers attributes
    select s.*, c.credit_limit
    from exploded s
    left join customers c
        on s.customer_id = c.id
),
enriched_carts as (
    -- Ranking and gap features
    select
        id,
        customer_id,
        created_at,
        v,
        dense_rank() over w_part as credit_limit_dense_rank,
        count(credit_limit) over (partition by customer_id) as customer_id_count_credit_limit
    from enriched
    window w_part as (partition by k order by credit_limit nulls first)
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        min(customer_id_count_credit_limit) over w_part as customer_id_min_customer_id_count_credit_limit,
        max(customer_id_count_credit_limit) over w_part as customer_id_max_customer_id_count_credit_limit,
        sum(credit_limit_dense_rank) over (partition by customer_id order by cast(created_at as bigint) range between 7200 preceding and current row) as credit_limit_dense_rank_sum_2h,
        round(customer_id_count_credit_limit / sum(customer_id_count_credit_limit) over (partition by customer_id), 4) as customer_id_count_credit_limit_share
    from enriched_carts
    where customer_id_count_credit_limit != 13
    window w_part as (partition by customer_id)
),
enriched_carts_2 as (
    -- Attach customers attributes
    select
        e.customer_id,
        e.created_at,
        e.credit_limit_dense_rank,
        e.customer_id_count_credit_limit,
        e.customer_id_min_customer_id_count_credit_limit,
        e.customer_id_max_customer_id_count_credit_limit,
        e.credit_limit_dense_rank_sum_2h,
        b.signup_date
    from with_windows e
    left join customers b
        on e.customer_id <=> b.id
)
select
    s.customer_id,
    s.credit_limit_dense_rank,
    s.customer_id_count_credit_limit,
    s.customer_id_min_customer_id_count_credit_limit,
    s.customer_id_max_customer_id_count_credit_limit,
    s.credit_limit_dense_rank_sum_2h,
    (select sum(x.discount) from orders x where x.customer_id = s.customer_id) as sum_order_discount
from enriched_carts_2 s
order by customer_id, customer_id_min_customer_id_count_credit_limit
