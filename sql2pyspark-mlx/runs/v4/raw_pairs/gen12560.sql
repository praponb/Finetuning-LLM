select a.account_type, b.status from (select distinct account_type from accounts) a cross join (select distinct status from subscriptions) b order by account_type, status;
