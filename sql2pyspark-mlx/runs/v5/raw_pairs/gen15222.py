from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sqft").desc_nulls_first())
w2 = Window.partitionBy("region").orderBy("sqft", "id")
w3 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("region").orderBy("opened_date", "id")

valid = (
    df_stores
    .filter(F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.lower("region").alias("region_lower"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.last_restock") <= F.col("s.opened_date")), "left_semi")
)

scored = (
    kept
    .select(
        "id",
        "store_name",
        F.rank().over(w).alias("sqft_rank"),
        F.lead("sqft", 1, 0).over(w2).alias("next_sqft"),
        F.sum("sqft").over(w3).alias("running_sqft"),
        F.lead("sqft", 1).over(w4).alias("next_sqft_2"),
        F.ntile(5).over(w2).alias("sqft_bucket"),
    )
)

eligible = (
    scored
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.product_id").isin([4, 6])), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("sqft_bucket") >= 8, "LARGE").otherwise("SMALL").alias("sqft_bucket_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("next_sqft_2").alias("avg_next_sqft_2"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("distinct_ids").desc_nulls_last())
)
