from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("region").orderBy("sqft", "id")
w4 = Window.partitionBy("region").orderBy("sqft_share", "id")
w5 = Window.partitionBy("region")
w_ordered = Window.partitionBy("city", "region").orderBy("sqft_share", "id")

with_windows = (
    df_stores
    .select(
        "*",
        F.round((F.col("sqft") / F.sum("sqft").over(w)), 4).alias("sqft_share"),
        F.sum("sqft").over(w2).alias("cumulative_sqft"),
        F.lead("sqft", 1).over(w3).alias("next_sqft"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.opened_date")), "left_anti")
)

metrics = (
    not_flagged
    .withColumn("next_sqft_share", F.lead("sqft_share", 1).over(w4))
    .withColumn("region_sum_next_sqft", F.sum("next_sqft").over(w5))
    .withColumn("next_sqft_share_2", F.lead("sqft_share", 1).over(w_ordered))
)

by_type = (
    metrics
    .groupBy("city")
    .pivot("region", ["South", "North", "West"])
    .agg(F.sum("region_sum_next_sqft").alias("amount"))
)

result = (
    by_type
    .select("city", "North", "West")
    .orderBy(F.col("North").desc())
)
