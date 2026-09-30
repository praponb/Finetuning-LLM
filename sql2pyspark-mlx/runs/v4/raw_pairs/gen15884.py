from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("lowest_price", "id")
w2 = Window.partitionBy("plan").orderBy("effective_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

unpacked = (
    df_price_tiers
    .filter(F.col("effective_from") > "2024-06-01")
    .select(
        "id",
        "plan",
        "effective_from",
        F.array_min("tier_prices").alias("lowest_price"),
        F.slice("tier_limits", 1, 2).alias("first_two_limits"),
    )
)

result = (
    unpacked
    .withColumn("next_lowest_price", F.lead("lowest_price", 1).over(w))
    .withColumn("cumulative_lowest_price", F.sum("lowest_price").over(w2))
    .withColumn("first_known_lowest_price", F.first("lowest_price", ignorenulls=True).over(w2))
)
