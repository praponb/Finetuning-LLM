from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("launch_date").desc(), "id")
w2 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("stock_qty", "id")
w4 = Window.partitionBy("brand").orderBy(F.col("stock_qty").desc())
w5 = Window.partitionBy("category")
w6 = Window.partitionBy("brand")
w7 = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("stock_qty_bucket", "id").rowsBetween(-6, Window.currentRow)
w_part = Window.partitionBy("city").orderBy(F.col("cumulative_rating").desc(), "id")

latest = (
    df_products
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

enriched = (
    latest
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.rating"), F.col("b.country"), F.col("b.city"))
)

metrics = (
    enriched
    .withColumn("cumulative_rating", F.sum("rating").over(w2))
    .withColumn("stock_qty_bucket", F.ntile(3).over(w3))
)

scored = (
    metrics
    .withColumn("stock_qty_dense_rank", F.dense_rank().over(w4))
    .withColumn("category_avg_price", F.avg("price").over(w5))
    .withColumn("brand_sum_stock_qty", F.sum("stock_qty").over(w6))
)

valid = (
    scored
    .filter(F.col("category") != "Sports")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "city",
        "cumulative_rating",
        "stock_qty_bucket",
        "stock_qty_dense_rank",
        "category_avg_price",
        F.greatest("cost", "brand_sum_stock_qty").alias("greatest_cost_brand_sum_stock_qty"),
    )
)

base = (
    valid
    .filter(F.col("brand").isNotNull())
    .select("*", F.concat_ws(",", "category", "city").alias("category_city_key"))
)

enriched_products = (
    base
    .withColumn("cumulative_stock_qty_dense_rank", F.sum("stock_qty_dense_rank").over(w7))
    .withColumn("stock_qty_dense_rank_avg_6", F.avg("stock_qty_dense_rank").over(w8))
    .withColumn("cumulative_rating_bucket", F.ntile(3).over(w_part))
)

result = (
    enriched_products
    .groupBy("city")
    .pivot("category", ["Electronics", "Books"])
    .agg(F.sum("stock_qty_dense_rank_avg_6").alias("amount"))
)
