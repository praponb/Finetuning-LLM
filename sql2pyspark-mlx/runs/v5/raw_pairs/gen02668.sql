select txn_type, round((100 * sum(case when merchant = 'Grab' then 1 else 0 end)) / count(*), 1) pct_grab from transactions group by txn_type order by txn_type, pct_grab
