from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("device_code").orderBy(F.col("humidity").desc())
w_running_2 = Window.partitionBy("device_code").orderBy(F.col("humidity").desc_nulls_first())

metrics = (
    df_device_readings
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "pressure",
        F.dense_rank().over(w_running).alias("humidity_dense_rank"),
        F.cume_dist().over(w_running_2).alias("humidity_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "device_code",
        "reading_ts",
        "humidity",
        "humidity_dense_rank",
        "humidity_cume_dist",
        F.when(F.col("device_code").rlike("^(DV-02|DV-01)$"), "GROUP_B").otherwise("CORE").alias("device_code_zone"),
    )
    .orderBy(F.col("reading_ts").desc_nulls_last())
)
