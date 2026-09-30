from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc_nulls_first())
w3 = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.initcap(F.trim("brand")).alias("brand_title"),
    )
)

cleaned = (
    valid
    .filter((~F.col("brand").eqNullSafe(F.lit("Acme"))) & F.col("supplier_id").isin([8, 12, 15]))
    .withColumn("tags_count", F.size("tags"))
    .withColumn("tags_text", F.array_join("tags", ", "))
)

enriched = (
    cleaned
    .filter(F.col("category") != "Books")
    .select(
        "*",
        F.avg("tags_count").over(w).alias("category_avg_tags_count"),
        F.cume_dist().over(w2).alias("cost_cume_dist"),
        F.first("stock_qty", ignorenulls=True).over(w3).alias("first_known_stock_qty"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left_anti")
)

result = (
    eligible
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("activity_month"), "brand")
    .agg(
        F.sum("price").alias("total_price"),
        F.count_if(F.col("brand") == "Stark").alias("stark_events"),
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_count"),
        F.count_if(F.col("category") == "Sports").alias("sports_events"),
    )
    .filter(F.col("stark_events") > 1)
    .orderBy(F.col("stark_events").desc())
)
