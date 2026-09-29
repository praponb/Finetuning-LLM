WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        device_code,
        pressure,
        status_bits,
        firmware,
        AVG(humidity) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_humidity,
        MIN(humidity) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_humidity,
        COUNT(humidity) OVER (PARTITION BY device_code ORDER BY pressure, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_humidity,
        LAST_VALUE(humidity, TRUE) OVER (PARTITION BY device_code ORDER BY humidity, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_humidity
    FROM device_readings
    WHERE device_code = 'DV-02'
),
matched_keys AS (
    -- Set intersection on keys
    SELECT firmware
    FROM enriched
    INTERSECT
    SELECT firmware
    FROM device_readings
    WHERE device_code != 'DV-04'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    INNER JOIN matched_keys k ON s.firmware = k.firmware
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        pressure,
        status_bits,
        firmware,
        running_min_humidity,
        running_count_humidity,
        last_known_humidity,
        CASE WHEN firmware = '2.0.1' THEN 1 ELSE 0 END AS is_2_0_1
    FROM kept_rows
    WHERE running_count_humidity >= 7
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        pressure,
        status_bits,
        running_count_humidity,
        is_2_0_1,
        CAST(pressure AS INT) AS pressure_int
    FROM cleaned
    WHERE firmware = '1.1.0'
),
kept_keys AS (
    -- Set difference on keys
    SELECT id
    FROM filtered
    EXCEPT ALL
    SELECT id
    FROM device_readings
    WHERE temp_c >= 30
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.id = k.id
)
SELECT
    id,
    pressure,
    status_bits,
    running_count_humidity,
    is_2_0_1
FROM reconciled
ORDER BY pressure DESC NULLS LAST;
