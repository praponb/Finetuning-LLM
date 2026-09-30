select id, amount from payments where amount <= (select min(amount) from payments where status in ('SUCCESS', 'REFUNDED', 'FAILED')) order by id limit 3;
