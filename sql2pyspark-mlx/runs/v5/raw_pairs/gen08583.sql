select id, reading_ts, pressure, status_bits from (select *, row_number() over (partition by device_code order by humidity nulls last, id) as row_num from device_readings) x where row_num = 1;
