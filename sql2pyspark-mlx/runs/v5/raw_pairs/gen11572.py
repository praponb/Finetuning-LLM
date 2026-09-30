from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.to_date(F.lit("2023-09-05"))) & F.col("channel").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
        F.coalesce("channel", F.lit("OTHER")).alias("resolved_channel"),
    )
)

result = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "channel", "promo_code", "ends_on", "resolved_channel")
)
