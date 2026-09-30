from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "browser").orderBy("started_at", "id")

base = (
    df_sessions
    .filter((F.col("started_at") >= F.to_timestamp(F.lit("2023-05-01 00:00:00"))) & (~F.col("country").eqNullSafe(F.lit("JP"))))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.hour("started_at").alias("started_at_hour"),
        F.coalesce("country", F.lit("OTHER")).alias("country_clean"),
    )
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
