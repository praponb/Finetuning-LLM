from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("supplier_id", "brand").orderBy(F.col("cost_bucket").asc_nulls_first())
w4 = Window.orderBy(F.col("cost_bucket").desc())
w5 = Window.partitionBy("category")

enriched = (
    df_products
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "tags",
        "supplier_id",
        F.first("cost", ignorenulls=True).over(w).alias("first_known_cost"),
        F.ntile(5).over(w2).alias("cost_bucket"),
    )
)

kept_keys = (
    enriched
    .select("supplier_id", "category")
    .exceptAll(
        df_products
        .filter(F.col("supplier_id").isNotNull())
        .select("supplier_id", "category")
    )
)

verified = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("sorted_tags", F.sort_array("tags"))
)

enriched_products = (
    filtered
    .select(
        "*",
        F.cume_dist().over(w3).alias("cost_bucket_cume_dist"),
        F.percent_rank().over(w4).alias("cost_bucket_percent_rank"),
        F.corr("stock_qty", "price").over(w5).alias("corr_stock_qty_price"),
    )
)

result = (
    enriched_products
    .groupBy("supplier_id")
    .pivot("category", ["Grocery", "Sports"])
    .agg(F.sum("cost_bucket_cume_dist").alias("amount"), F.count(F.lit(1)).alias("n"))
)
