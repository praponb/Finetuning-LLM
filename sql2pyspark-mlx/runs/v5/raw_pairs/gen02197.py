from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy(F.col("started_at").desc_nulls_first(), "id")

base = (
    df_sessions
    .filter(~F.col("country").eqNullSafe(F.lit("TH")))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.concat("device", F.lit(" - "), "browser").alias("device_browser"),
    )
)

result = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
