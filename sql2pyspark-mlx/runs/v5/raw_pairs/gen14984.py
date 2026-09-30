from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("device").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy(F.col("started_at_date").desc(), "id")
w3 = Window.partitionBy("browser", "device").orderBy("started_at_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-07-16").cast("date")) & (F.col("country").eqNullSafe(F.lit("TH"))))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        F.to_date("started_at").alias("started_at_date"),
        F.lower("device").alias("device_lower"),
        F.coalesce("country", F.lit("OTHER")).alias("resolved_country"),
    )
)

result = (
    valid
    .filter(F.col("started_at_date") > "2024-08-01")
    .withColumn("seq_no", F.row_number().over(w))
    .withColumn("seq_no_2", F.row_number().over(w2))
    .withColumn("last_known_browser", F.last("browser", ignorenulls=True).over(w3))
)
