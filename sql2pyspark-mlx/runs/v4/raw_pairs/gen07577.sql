with eligible as (
    select s.*
    from accounts s
    where not exists (
            select 1
            from transactions x
            where x.account_id = s.id
              and x.txn_time < s.opened_date
        )
)
select
    date_trunc('year', opened_date) as year_start,
    account_type,
    status,
    stddev_samp(balance) as balance_stddev,
    count(*) filter (where status = 'OPEN') as open_rows,
    count(distinct account_type) as distinct_account_types,
    count(case when account_type = 'CHECKING' then 1 end) as checking_count,
    stddev_samp(balance) as balance_stddev_2,
    dense_rank() over (partition by date_trunc('year', opened_date) order by stddev_samp(balance) desc) as balance_stddev_dense_rank
from eligible
group by date_trunc('year', opened_date), account_type, status;
