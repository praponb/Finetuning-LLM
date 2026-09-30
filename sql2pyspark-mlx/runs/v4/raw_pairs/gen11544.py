from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("started_at").desc_nulls_last(), "id")

valid = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-09-05").cast("date"))
    .select("*", F.concat_ws("/", "browser", "device").alias("browser_device_key"))
)

result = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)
