select account_type, cnt from (select account_type, count(*) as cnt from accounts group by account_type) as sub where cnt > 6;
