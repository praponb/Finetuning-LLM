with ranked as (select *, row_number() over (partition by store_id order by quantity desc, id) as rnk from inventory where month(last_restock) in (2, 6, 11)) select * from ranked where rnk <= 2;
