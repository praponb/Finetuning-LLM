with latest as (
    -- QUALIFY-style window filter
    select *, rank() over (partition by segment order by age desc) as rn
    from customers
    qualify rn = 1
),
with_windows as (
    -- Ranking and gap features
    select
        *,
        sum(age) over (partition by segment order by age, id rows between unbounded preceding and current row) as cumulative_age,
        avg(credit_limit) over w_grp as credit_limit_avg_5_prior
    from latest
    window w_grp as (partition by city order by rn, id rows between 5 preceding and 1 preceding)
),
with_windows_customers as (
    -- Window metrics over several different partitions
    select
        *,
        last_value(segment, true) over w_recent as last_known_segment,
        sum(cumulative_age) over (order by rn, id rows between unbounded preceding and current row) as running_cumulative_age,
        row_number() over (partition by segment, country order by credit_limit_avg_5_prior, id) as seq_no,
        last_value(cumulative_age) ignore nulls over (partition by city, country order by signup_date, id rows between unbounded preceding and current row) as last_known_cumulative_age
    from with_windows
    window w_recent as (order by cumulative_age, id rows between unbounded preceding and current row)
),
with_last as (
    -- Latest matching order per row (LATERAL ... LIMIT 1)
    select
        s.*,
        x.order_discount,
        x.amount
    from with_windows_customers s
    left join lateral (
            select c.discount as order_discount, c.amount
            from orders c
            where c.customer_id = s.id
            order by c.shipping_fee desc, c.id desc
            limit 1
        ) x on true
),
with_windows_customers_2 as (
    -- Running and moving aggregates
    select
        id,
        name,
        email,
        country,
        city,
        segment,
        signup_date,
        is_active,
        credit_limit,
        age,
        rn,
        cumulative_age,
        credit_limit_avg_5_prior,
        last_known_segment,
        running_cumulative_age,
        seq_no,
        last_known_cumulative_age,
        order_discount,
        amount,
        avg(order_discount) over (partition by segment order by age, id rows between 6 preceding and current row) as order_discount_avg_6,
        ntile(10) over (partition by city order by last_known_cumulative_age desc, id) as last_known_cumulative_age_bucket,
        round(running_cumulative_age / sum(running_cumulative_age) over (partition by country), 4) as running_cumulative_age_share,
        sum(cumulative_age) over (partition by segment, country) as segment_sum_cumulative_age
    from with_last
)
select
    id,
    name,
    country,
    segment,
    signup_date,
    is_active,
    credit_limit,
    age,
    rn
from with_windows_customers_2
order by id desc nulls last;
