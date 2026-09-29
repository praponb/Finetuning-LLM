select *
from (
    select
        is_flagged,
        txn_type,
        round((100 * sum(case when merchant = 'Shell' then 1 else 0 end)) / count(*), 1) as pct_shell,
        max(txn_time) as latest_txn_time
    from transactions
    where amount between 1300 and 3000
    group by is_flagged, txn_type
) as sub
where pct_shell >= 67
