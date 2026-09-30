select *, stddev_samp(balance) over (partition by customer_id) as customer_id_stddev_samp_balance, row_number() over (partition by status order by balance desc, id) as seq_no from accounts;
