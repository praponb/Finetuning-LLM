select * from (select *, rank() over (partition by account_id order by amount) as rnk from transactions) as ranked where rnk = 1 order by amount
