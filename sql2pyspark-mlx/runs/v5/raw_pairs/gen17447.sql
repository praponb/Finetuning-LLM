with kept_keys as (
    select txn_type, id
    from transactions
    except all
    select txn_type, id
    from transactions
    where txn_type = 'CREDIT'
),
verified as (
    select s.*
    from transactions s
    join kept_keys k on s.txn_type = k.txn_type and s.id = k.id
)
select
    txn_type,
    stddev_samp(amount) as amount_stddev,
    count(case when merchant = '7-Eleven' then 1 end) as 7_eleven_count,
    stddev_samp(amount) as amount_stddev_2,
    max(txn_time) as last_txn_time,
    sort_array(collect_set(txn_type)) as txn_type_list,
    (unix_timestamp(max(txn_time)) - unix_timestamp(min(txn_time))) / 60 as span_minutes
from verified
group by txn_type
order by amount_stddev_2 nulls first;
