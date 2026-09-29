from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("discount_pct").desc())

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2024-04-07").cast("date")) & F.col("channel").isNotNull())
    .withColumn("channel_lower", F.lower("channel"))
)

result = (
    filtered
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)
