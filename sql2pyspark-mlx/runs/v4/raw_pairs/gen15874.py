from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device_code").orderBy("status_bits", "id")

valid = (
    df_device_readings
    .filter((F.col("reading_ts") > F.to_date(F.lit("2025-09-01"))) & (F.col("pressure") >= 1027))
    .select(
        "id",
        "device_code",
        "pressure",
        "status_bits",
        "firmware",
        F.round((F.col("status_bits") * 1.1), 2).alias("adjusted_status_bits"),
    )
)

result = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)
