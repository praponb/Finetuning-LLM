select *, concat_ws('-', status, carrier) as status_carrier_key, case when carrier = 'Flash' then 1 else 0 end as is_flash from shipments order by is_flash desc, delivered_date;
