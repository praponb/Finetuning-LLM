from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("tags_count", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy("tags_count")
w3 = Window.partitionBy("category").orderBy("tags_count", "id")
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.avg("cost").alias("avg_product_cost"))

valid = (
    df_products
    .filter(F.col("brand").isNotNull() & F.col("price").between(1070, 1100))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.upper(F.trim("product_name")).alias("product_name_normalized"),
        F.coalesce("price", F.lit(0)).alias("price_filled"),
    )
)

base = (
    valid
    .filter(F.col("cost") <= 650)
    .withColumn("launch_date_label", F.date_format("launch_date", "MMM yyyy"))
)

valid_products = (
    base
    .filter(F.col("stock_qty").between(313, 343))
    .withColumn("tags_count", F.size("tags"))
)

metrics = (
    valid_products
    .select(
        "*",
        F.first("tags_count", ignorenulls=True).over(w).alias("first_known_tags_count"),
        F.dense_rank().over(w2).alias("tags_count_dense_rank"),
    )
)

ranked = (
    metrics
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

kept = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([8, 10])), "left_anti")
)

stats = (
    kept
    .groupBy(
        F.when(F.col("cost") >= 790, "HIGH").otherwise("LOW").alias("cost_tier"),
        "supplier_id",
        "category",
    )
    .agg(
        F.round(F.sum("first_known_tags_count") / F.nullif(F.count(F.when(F.col("category") == "Electronics", 1)), F.lit(0)), 2).alias("first_known_tags_count_per_electronics"),
        F.count_if(F.col("category") == "Electronics").alias("electronics_events"),
    )
)

result = (
    stats
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select(
        "cost_tier",
        "supplier_id",
        "category",
        "first_known_tags_count_per_electronics",
        "electronics_events",
        F.col("avg_product_cost"),
    )
    .orderBy(F.col("avg_product_cost").desc_nulls_last(), "cost_tier")
)
