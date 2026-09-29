from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy(F.col("days_since_started_at").desc_nulls_last(), "id")

filtered = (
    df_sessions
    .filter(F.col("started_at") < "2025-08-01")
    .select(
        "id",
        "customer_id",
        "browser",
        "country",
        F.datediff(F.current_date(), "started_at").alias("days_since_started_at"),
        F.coalesce("country", F.lit("NONE")).alias("country_filled"),
    )
)

result = (
    filtered
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)
