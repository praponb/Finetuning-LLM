with numbered as (select *, rank() over (partition by txn_type order by amount desc) as pos from transactions) select id, txn_type, amount from numbered where pos <= 3 order by txn_type asc
