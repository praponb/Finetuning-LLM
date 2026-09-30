with with_dims as (
    -- Attach customers attributes
    select e.*, p.segment
    from subscriptions e
    inner join customers p
        on e.customer_id = p.id
    where p.email is not null
),
joined as (
    -- Attach customers attributes
    select
        a.id,
        a.customer_id,
        a.plan,
        a.end_date,
        a.monthly_fee,
        a.status,
        a.segment,
        p.signup_date,
        p.country
    from with_dims a
    left join customers p
        on a.customer_id = p.id
),
enriched as (
    -- Attach customers attributes
    select
        s.*,
        p.email,
        p.age,
        p.credit_limit
    from joined s
    inner join customers p
        on s.customer_id = p.id
),
deduped as (
    -- Keep one row per key (latest first)
    select
        id,
        plan,
        end_date,
        monthly_fee,
        status,
        email,
        age,
        credit_limit
    from (
        select *, rank() over (partition by plan order by end_date desc) as rn
        from enriched
    ) x
    where rn = 1
),
with_windows as (
    -- Window metrics over several different partitions
    select
        id,
        end_date,
        monthly_fee,
        email,
        sum(credit_limit) over w_hist running_sum_credit_limit,
        max(credit_limit) over w_hist running_max_credit_limit,
        count(credit_limit) over w_hist running_count_credit_limit,
        sum(credit_limit) over (partition by plan order by age, id rows between unbounded preceding and current row) running_credit_limit
    from deduped
    window w_hist as (partition by status order by monthly_fee, id rows between unbounded preceding and current row)
),
cleaned as (
    -- Filter the raw rows and derive helper columns
    select
        id,
        end_date,
        monthly_fee,
        running_sum_credit_limit,
        running_max_credit_limit,
        running_count_credit_limit,
        running_credit_limit * running_count_credit_limit running_credit_limit_times_running_count_credit_limit
    from with_windows
    where end_date >= date '2023-04-07'
)
select
    id,
    end_date,
    monthly_fee,
    running_sum_credit_limit,
    running_max_credit_limit,
    running_credit_limit_times_running_count_credit_limit
from cleaned
order by running_credit_limit_times_running_count_credit_limit desc nulls first;
