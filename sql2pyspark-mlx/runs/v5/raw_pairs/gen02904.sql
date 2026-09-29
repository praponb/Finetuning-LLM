SELECT device_code, id
FROM device_readings
EXCEPT DISTINCT
SELECT device_code, id
FROM device_readings
WHERE status_bits > 9;
