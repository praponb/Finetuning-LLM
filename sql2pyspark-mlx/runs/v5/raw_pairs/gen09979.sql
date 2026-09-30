SELECT firmware, id
FROM device_readings
INTERSECT
SELECT firmware, id
FROM device_readings
WHERE pressure BETWEEN 1008 AND 1016;
