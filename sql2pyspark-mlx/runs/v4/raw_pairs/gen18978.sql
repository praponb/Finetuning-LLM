with numbered as (select *, row_number() over (partition by customer_id order by balance desc, id) as rnk from accounts) select id, customer_id, balance, opened_date from numbered where rnk <= 3;
