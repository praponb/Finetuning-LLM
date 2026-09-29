select id, amount, is_flagged, account_id from transactions where amount >= (select max(amount) from transactions);
