select a.status, min(t.amount) as min_amount from transactions t left join accounts a on t.account_id = a.id group by a.status order by a.status
