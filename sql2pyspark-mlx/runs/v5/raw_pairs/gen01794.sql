SELECT firmware, device_code
FROM device_readings
EXCEPT
SELECT firmware, device_code
FROM device_readings
WHERE firmware <> '1.0.3';
