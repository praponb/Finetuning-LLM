select a.account_type, t.merchant from accounts a left join transactions t on a.id = t.account_id order by account_type desc
