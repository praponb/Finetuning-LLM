select id, status, amount, method from (select id, status, amount, method, rank() over (partition by status order by amount desc) as rnk from payments) as r where rnk <= 2
