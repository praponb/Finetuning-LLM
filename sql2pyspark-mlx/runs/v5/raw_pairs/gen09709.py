from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("device_length").desc_nulls_last(), "id")

cleaned = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-04-08").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.length("device").alias("device_length"),
        F.upper("device").alias("device_upper"),
    )
)

result = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)
