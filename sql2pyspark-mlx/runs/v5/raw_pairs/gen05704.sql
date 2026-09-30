SELECT id, store_id, kpi, reading FROM (SELECT id, store_id, discount, amount, shipping_fee FROM orders) src UNPIVOT (reading FOR kpi IN (discount, amount, shipping_fee AS SHIPPING_FEE_VAL))
