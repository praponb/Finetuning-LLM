select id, txn_type, txn_time, ntile(4) over (partition by txn_type order by txn_time, id) as txn_time_ntile from transactions order by id desc;
