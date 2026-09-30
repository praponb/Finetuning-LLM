select id, method, paid_at, amount from (select id, method, paid_at, amount, rank() over (partition by method order by paid_at) as rnk from payments) as r where rnk <= 2 order by amount desc;
