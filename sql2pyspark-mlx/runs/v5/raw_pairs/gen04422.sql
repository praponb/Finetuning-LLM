select id, delivered_date, shipped_date, initcap(trim(status)) as status_title, date_format(shipped_date, 'yyyy-MM-dd') as shipped_date_label from shipments where order_id is null
