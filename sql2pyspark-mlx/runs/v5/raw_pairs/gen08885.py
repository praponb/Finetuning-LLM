from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("started_at").desc(), "id")

cleaned = (
    df_sessions
    .filter(F.col("started_at") >= F.lit("2023-04-21 02:00:00").cast("timestamp"))
    .select(
        "id",
        "customer_id",
        "browser",
        "started_at",
        "country",
        F.length("country").alias("country_length"),
        F.coalesce("country", F.lit("UNKNOWN")).alias("resolved_country"),
    )
)

result = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)
