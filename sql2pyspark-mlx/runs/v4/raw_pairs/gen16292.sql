select t.account_id, t.merchant, a.status, a.balance, a.opened_date from transactions t right join accounts a on t.account_id = a.id where t.txn_type = 'CREDIT' order by t.merchant;
