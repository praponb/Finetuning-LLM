with filtered_accounts as (
    select *
    from accounts
    where not (customer_id = 7 and balance <= 61000)
),
Agg as (
    select
        case when balance >= 70000 then 'high' when balance >= 21000 then 'mid' else 'low' end as balance_bucket,
        account_type,
        concat_ws(', ', sort_array(collect_set(account_type))) as account_type_values,
        round((100 * sum(case when status = 'CLOSED' then 1 else 0 end)) / count(*), 1) as pct_closed
    from filtered_accounts
    group by case when balance >= 70000 then 'high' when balance >= 21000 then 'mid' else 'low' end, account_type
)
select balance_bucket, account_type_values, pct_closed
from Agg
order by balance_bucket asc, pct_closed asc;
