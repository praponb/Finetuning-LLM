SELECT channel, COUNT(DISTINCT amount) AS unique_channels FROM orders GROUP BY channel ORDER BY unique_channels;
