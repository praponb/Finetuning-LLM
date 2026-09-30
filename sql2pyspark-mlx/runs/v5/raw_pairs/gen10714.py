from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category").orderBy("product_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("category").orderBy(F.col("product_price").desc(), "id")
w4 = Window.partitionBy("category")
w5 = Window.partitionBy("category").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("category").orderBy(F.col("rating").desc())
w7 = Window.partitionBy("product_brand")
w8 = Window.orderBy("rating_dense_rank", "id")
w9 = Window.partitionBy("category", "product_brand")

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.rating",
        F.col("c.category"),
        F.col("c.price").alias("product_price"),
    )
)

enriched = (
    with_last
    .select(
        "id",
        "supplier_name",
        "category",
        "product_price",
        F.sum("rating").over(w2).alias("cumulative_rating"),
        F.row_number().over(w_part).alias("row_seq"),
        F.round((F.col("product_price") / F.sum("product_price").over(w4)), 4).alias("product_price_share"),
        F.last("product_price", ignorenulls=True).over(w5).alias("last_known_product_price"),
        F.dense_rank().over(w6).alias("rating_dense_rank"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.category",
        "s.rating_dense_rank",
        F.col("c.brand").alias("product_brand"),
    )
)

with_windows = (
    with_latest
    .select(
        "id",
        "supplier_name",
        "category",
        "product_brand",
        F.stddev_samp("rating_dense_rank").over(w7).alias("product_brand_stddev_samp_rating_dense_rank"),
        F.ntile(5).over(w8).alias("rating_dense_rank_bucket"),
        F.round((F.col("rating_dense_rank") / F.sum("rating_dense_rank").over(w9)), 4).alias("rating_dense_rank_share"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("product_brand_stddev_samp_rating_dense_rank") >= 8, "HIGH").otherwise("LOW").alias("product_brand_stddev_samp_rating_dense_rank_tier"),
        F.when(F.col("rating_dense_rank_bucket") >= 7, "HIGH").otherwise("LOW").alias("rating_dense_rank_bucket_tier"),
    )
    .agg(
        F.avg(F.when(F.col("product_brand") == "Stark", F.col("rating_dense_rank_share"))).alias("stark_avg_rating_dense_rank_share"),
        F.count_if(F.col("product_brand") == "Umbrella").alias("umbrella_events"),
        F.avg(F.when(F.col("category") == "Books", F.col("rating_dense_rank_share"))).alias("books_avg_rating_dense_rank_share"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_rows"),
        F.size(F.collect_set("product_brand")).alias("num_product_brands"),
    )
    .orderBy("books_avg_rating_dense_rank_share")
)
