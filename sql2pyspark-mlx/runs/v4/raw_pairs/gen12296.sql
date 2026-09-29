select id, amount, txn_type, merchant from transactions where amount > (select avg(amount) from transactions where account_id in (17, 25, 40))
