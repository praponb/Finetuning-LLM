with cleaned as (
    select
        id,
        customer_id,
        balance,
        concat(account_type, '/', status) as account_type_status,
        coalesce(status, 'N/A') as resolved_status
    from accounts
    where status is not null
),
deduped as (
    select *
    from cleaned
    qualify row_number() over (partition by customer_id order by balance desc, id) = 1
),
base as (
    select
        id,
        customer_id,
        balance,
        resolved_status,
        case when balance >= 61000 then 'High' when balance >= 34000 then 'Medium' else 'Low' end as balance_band,
        round(balance * 1.15, 2) as adjusted_balance
    from deduped
    where customer_id is null
),
scored as (
    select
        *,
        row_number() over w_grp as seq_no,
        sum(balance) over (partition by customer_id) as customer_id_sum_balance,
        lead(adjusted_balance, 1) over w_grp as next_adjusted_balance,
        first_value(balance) ignore nulls over w_grp_2 as first_known_balance
    from base
    where adjusted_balance between 56 and 71
    window w_grp as (partition by customer_id order by adjusted_balance, id), w_grp_2 as (order by adjusted_balance, id rows between unbounded preceding and current row)
),
cleaned_accounts as (
    select
        id,
        customer_id,
        balance,
        balance_band,
        adjusted_balance,
        customer_id_sum_balance,
        next_adjusted_balance,
        first_known_balance,
        greatest(first_known_balance, adjusted_balance) as greatest_first_known_balance_adjusted_balance,
        customer_id_sum_balance * balance as customer_id_sum_balance_times_balance
    from scored
    where first_known_balance between 60000 and 62000
),
kept_keys as (
    select id
    from cleaned_accounts
    intersect
    select id
    from accounts
    where customer_id = 10
),
verified as (
    select s.*
    from cleaned_accounts s
    join kept_keys k on s.id = k.id
)
select
    customer_id,
    count(*) as num_rows,
    min(balance) as min_balance,
    percentile_approx(next_adjusted_balance, 0.95) as next_adjusted_balance_p95,
    percentile_approx(next_adjusted_balance, 0.95) as next_adjusted_balance_p95_2,
    round(avg(customer_id_sum_balance_times_balance), 2) as avg_customer_id_sum_balance_times_balance,
    percentile_approx(greatest_first_known_balance_adjusted_balance, 0.5) as greatest_first_known_balance_adjusted_balance_p50
from verified
group by customer_id
