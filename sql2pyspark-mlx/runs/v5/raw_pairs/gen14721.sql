select *
from (
    select
        is_flagged,
        merchant,
        sum(case when txn_type = 'CREDIT' then 1 else 0 end) as credit_count,
        count(case when txn_type = 'REFUND' then 1 end) as refund_count
    from transactions
    where is_flagged = true
    group by is_flagged, merchant
) as x
where refund_count >= 7
