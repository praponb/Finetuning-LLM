select transactions.id, transactions.merchant, accounts.account_type, accounts.customer_id, accounts.opened_date from transactions inner join accounts on transactions.account_id = accounts.id
