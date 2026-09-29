with ranked_accounts as (select id, account_type, balance, row_number() over (partition by account_type order by balance desc, id) as rnk from accounts) select * from ranked_accounts where rnk <= 5;
