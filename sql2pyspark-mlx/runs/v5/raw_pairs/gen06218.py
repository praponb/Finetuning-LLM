from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "device").orderBy(F.col("started_at").desc(), "id")

cleaned = (
    df_sessions
    .filter(F.col("browser").isNotNull() & (F.col("started_at") >= F.lit("2023-09-21 18:00:00").cast("timestamp")))
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "started_at",
        F.to_date("started_at").alias("started_at_date"),
        F.coalesce("country", F.lit("OTHER")).alias("country_clean"),
    )
)

result = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)
