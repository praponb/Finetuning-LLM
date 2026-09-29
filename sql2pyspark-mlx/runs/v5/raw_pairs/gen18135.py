from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w2 = Window.partitionBy("category").orderBy(F.col("sports_rows").desc())

deduped = (
    df_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
    )
)

result = (
    deduped
    .groupBy("category", F.date_trunc("week", "launch_date").alias("week_start"))
    .agg(
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_rows"),
        F.max(F.when(F.col("brand") == "Acme", F.col("price"))).alias("acme_max_price"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
    )
    .withColumn("sports_rows_dense_rank", F.dense_rank().over(w2))
    .orderBy(F.col("sports_rows").desc())
)
