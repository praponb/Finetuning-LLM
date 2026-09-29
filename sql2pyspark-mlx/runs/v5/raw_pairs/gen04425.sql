select id, store_id, discount, rank() over (partition by store_id order by discount desc) rnk from orders;
