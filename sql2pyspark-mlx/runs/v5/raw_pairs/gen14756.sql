select quantity, order_id, case when unit_price >= 390 then 'High' when unit_price >= 110 then 'Medium' else 'Low' end unit_price_band from order_items where quantity between 4 and 6
