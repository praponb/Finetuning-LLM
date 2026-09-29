select id, txn_type, amount, ntile(4) over (partition by txn_type order by amount desc, id) quartile from transactions where amount <= 1300;
