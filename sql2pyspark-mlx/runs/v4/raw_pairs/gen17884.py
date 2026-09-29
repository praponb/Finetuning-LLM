from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("launch_date", "id")
w2 = Window.partitionBy("brand").orderBy(F.col("launch_date").desc_nulls_first(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("supplier_id", "category").orderBy(F.col("launch_date").desc_nulls_first())
w5 = Window.orderBy("price", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("category").orderBy(F.col("price").asc_nulls_first())
w7 = Window.orderBy(F.col("price").desc(), "id")
w_grp = Window.orderBy(F.col("least_stock_qty_price").desc(), "id")
w_hist = Window.orderBy(F.col("least_stock_qty_price").desc())

base = (
    df_products
    .filter(F.col("supplier_id") == 33)
    .withColumn("price_level", F.when(F.col("price") >= 970, "High").when(F.col("price") >= 460, "Medium").otherwise("Low"))
)

filtered = (
    base
    .filter((F.col("launch_date") > F.lit("2023-08-05").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "launch_date",
        "supplier_id",
        "price_level",
        F.least("stock_qty", "price").alias("least_stock_qty_price"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

latest_match = (
    ranked
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock"))
)

latest_products = (
    latest_match
    .withColumn("rank_in_group", F.rank().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    latest_products
    .select(
        "*",
        F.avg("least_stock_qty_price").over(w5).alias("least_stock_qty_price_avg_2_prior"),
        F.dense_rank().over(w6).alias("price_dense_rank"),
        F.row_number().over(w7).alias("row_seq"),
        F.ntile(5).over(w_grp).alias("least_stock_qty_price_bucket"),
        F.dense_rank().over(w_hist).alias("least_stock_qty_price_dense_rank"),
    )
)

result = (
    metrics
    .groupBy("supplier_id")
    .pivot("brand", ["Umbrella", "Stark"])
    .agg(F.avg("least_stock_qty_price_dense_rank").alias("value_sum"))
    .select(
        "supplier_id",
        F.col("Umbrella").alias("umbrella"),
        F.col("Stark").alias("stark"),
    )
)
