from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("discount_pct")

cleaned = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2023-08-08").cast("date")) & (F.col("ends_on") > "2024-06-01"))
    .select(
        "*",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

result = (
    cleaned
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)
