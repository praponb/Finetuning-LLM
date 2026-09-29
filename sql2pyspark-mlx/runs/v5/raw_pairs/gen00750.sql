select id, txn_time, case when merchant = 'Lazada' then 1 else 0 end as is_lazada from transactions order by txn_time desc
