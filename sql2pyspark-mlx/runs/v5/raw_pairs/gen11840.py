from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("is_tablet").desc_nulls_last(), "id")

base = (
    df_sessions
    .filter(F.col("customer_id").isNotNull() & (F.col("started_at") >= F.lit("2023-02-21 11:00:00").cast("timestamp")))
    .select(
        "*",
        F.when(F.col("device") == "tablet", 1).otherwise(0).alias("is_tablet"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("browser_filled"),
    )
)

result = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "browser", "started_at", "country", "is_tablet")
)
