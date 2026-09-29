select *, row_number() over (partition by device_code order by pressure nulls last, id) as rn
from device_readings
qualify rn = 1;
