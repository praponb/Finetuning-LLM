select t.account_id, a.customer_id, a.opened_date, datediff(current_date(), a.opened_date) days_since_opened_date from transactions t full outer join accounts a on t.account_id = a.id;
