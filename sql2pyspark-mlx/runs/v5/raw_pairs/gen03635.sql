select id, store_id, shipping_fee, discount, case when discount >= 0.44 then 'High' when discount >= 0.17 then 'Medium' else 'Low' end as discount_level from orders
