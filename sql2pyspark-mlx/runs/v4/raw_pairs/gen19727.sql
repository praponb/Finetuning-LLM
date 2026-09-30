select account_id from transactions where account_id = 13 union all select account_id from transactions where is_flagged order by account_id
