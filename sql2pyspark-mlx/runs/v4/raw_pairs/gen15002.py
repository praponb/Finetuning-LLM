from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("last_restock_plus_months", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("store_id").orderBy(F.col("quantity").asc_nulls_last())
w5 = Window.partitionBy("product_id").orderBy("quantity", "id")
w6 = Window.orderBy("last_restock", "id")
w7 = Window.partitionBy("store_id").orderBy(F.col("quantity_bucket_2").desc_nulls_first(), "id")

cleaned = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-10-17").cast("date")) & F.col("product_id").isin([1, 4, 6]))
    .select("*", F.add_months("last_restock", 3).alias("last_restock_plus_months"))
)

scored = (
    cleaned
    .withColumn("last_known_quantity", F.last("quantity", ignorenulls=True).over(w))
    .withColumn("quantity_avg_6", F.avg("quantity").over(w2))
)

enriched = (
    scored
    .select(
        "*",
        F.ntile(5).over(w_grp).alias("quantity_bucket"),
        F.rank().over(w4).alias("quantity_rank"),
        F.ntile(10).over(w5).alias("quantity_bucket_2"),
        F.lag("quantity", 1).over(w6).alias("prev_quantity"),
    )
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = (
    ranked
    .withColumn("quarter_start", F.date_trunc("quarter", "last_restock"))
    .withColumn("last_known_quantity_tier", F.when(F.col("last_known_quantity") >= 243, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["quarter_start", "last_known_quantity_tier"], ["quarter_start"]],
        "quarter_start",
        "last_known_quantity_tier",
    )
    .agg(
        F.max("quantity_avg_6").alias("max_quantity_avg_6"),
        F.count("*").alias("event_count"),
        F.grouping("quarter_start").alias("gid"),
    )
)
