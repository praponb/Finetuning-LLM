from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("device").orderBy("device_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("browser")
w_recent = Window.partitionBy("customer_id").orderBy(F.col("device_length").desc(), "id")
w4 = Window.partitionBy("customer_id", "country").orderBy(F.col("device_length").desc_nulls_first())
w_grp = Window.orderBy(F.col("device_length").desc())

base = (
    df_sessions
    .filter((F.col("started_at") > F.to_timestamp(F.lit("2023-04-03 07:00:00"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        "country",
        F.length("device").alias("device_length"),
    )
)

result = (
    base
    .select(
        "*",
        F.last("country", ignorenulls=True).over(w_part).alias("last_known_country"),
        F.count("device_length").over(w2).alias("browser_count_device_length"),
        F.ntile(10).over(w_recent).alias("device_length_bucket"),
        F.dense_rank().over(w4).alias("device_length_dense_rank"),
        F.rank().over(w_grp).alias("device_length_rank"),
    )
)
