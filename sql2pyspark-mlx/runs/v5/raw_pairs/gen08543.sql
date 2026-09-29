select txn_type, count(case when txn_type = 'FEE' then 1 end) as fee_count, count(case when txn_type = 'REFUND' then 1 end) as refund_count from transactions group by txn_type order by fee_count desc
