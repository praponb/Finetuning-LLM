SELECT firmware, id
FROM device_readings
INTERSECT ALL
SELECT firmware, id
FROM device_readings
WHERE temp_c < 38
