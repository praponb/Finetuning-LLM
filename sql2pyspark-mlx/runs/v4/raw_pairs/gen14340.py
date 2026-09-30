from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "category").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("units", "id")
w3 = Window.orderBy(F.col("revenue").desc(), "id")
w4 = Window.partitionBy("category").orderBy("revenue", "id").rowsBetween(-5, -1)
w5 = Window.partitionBy("product_id").orderBy(F.col("units_bucket").desc_nulls_last(), "id")
w6 = Window.partitionBy("product_id").orderBy("revenue_bucket", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("category").orderBy("sale_date", "id")
w8 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("store_id").orderBy("revenue_avg_5_prior", "id")
w10 = Window.partitionBy("category", "store_id").orderBy("first_known_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("product_id", "category").orderBy(F.col("first_known_units_bucket").desc(), "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

enriched = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.category"))
)

metrics = (
    enriched
    .select(
        "*",
        F.first("revenue", ignorenulls=True).over(w).alias("first_known_revenue"),
        F.ntile(10).over(w2).alias("units_bucket"),
        F.ntile(5).over(w3).alias("revenue_bucket"),
        F.avg("revenue").over(w4).alias("revenue_avg_5_prior"),
    )
)

ranked = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

enriched_store_sales = (
    ranked
    .withColumn("revenue_avg_5_prior_avg_5", F.avg("revenue_avg_5_prior").over(w6))
    .withColumn("prev_units_bucket", F.lag("units_bucket", 2).over(w7))
)

scored = (
    enriched_store_sales
    .withColumn("running_revenue_bucket", F.sum("revenue_bucket").over(w8))
    .withColumn("seq_no", F.row_number().over(w9))
    .withColumn("first_known_units_bucket", F.first("units_bucket", ignorenulls=True).over(w10))
)

deduped = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w11))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "first_known_revenue",
        "units_bucket",
        "revenue_bucket",
        F.col("avg_order_item_quantity"),
    )
)
