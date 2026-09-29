select device_code, id
from device_readings
except all
select device_code, id
from device_readings
where status_bits < 9;
