select * from (select id, txn_type, amount, row_number() over (partition by txn_type order by amount desc, id) as row_num from transactions) as ranked where row_num = 1;
