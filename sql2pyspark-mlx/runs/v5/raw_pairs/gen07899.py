from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.orderBy(F.col("product_cost").desc_nulls_first())
w3 = Window.partitionBy("product_id").orderBy(F.col("price").desc(), "id")
w4 = Window.orderBy(F.col("quantity_filled").desc(), "id")
w5 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id").orderBy("product_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy("seq_no", "id")

filtered = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-08-09").cast("date")) & (F.col("store_id") == 1))
    .select("*", F.coalesce("quantity", F.lit(0)).alias("quantity_filled"))
)

base = (
    filtered
    .filter(F.col("store_id").isin([2, 4, 5]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_filled",
        F.date_trunc("YEAR", "last_restock").alias("last_restock_period"),
        F.when(F.col("quantity") >= 224, "High").when(F.col("quantity") >= 98, "Medium").otherwise("Low").alias("quantity_tier"),
    )
)

with_dims = (
    base
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.product_id",
        "s.quantity_filled",
        "s.last_restock_period",
        "s.quantity_tier",
        F.col("d.price"),
        F.col("d.cost"),
        F.col("d.product_name"),
    )
)

joined = (
    with_dims
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.category") != "Books")
    .select("e.*", F.col("d.cost").alias("product_cost"))
)

metrics = (
    joined
    .select(
        "*",
        F.avg("cost").over(w).alias("product_id_avg_cost"),
        F.rank().over(w2).alias("product_cost_rank"),
        F.row_number().over(w3).alias("seq_no"),
        F.ntile(4).over(w4).alias("quantity_filled_bucket"),
        F.first("cost", ignorenulls=True).over(w5).alias("first_known_cost"),
    )
)

metrics_inventory = (
    metrics
    .withColumn("running_max_product_cost_rank", F.max("product_cost_rank").over(w_recent))
    .withColumn("running_count_product_cost_rank", F.count("product_cost_rank").over(w_recent))
    .withColumn("running_sum_product_cost_rank", F.sum("product_cost_rank").over(w_recent))
    .withColumn("running_min_product_cost_rank", F.min("product_cost_rank").over(w_recent))
    .withColumn("prev_product_cost_rank", F.lag("product_cost_rank", 1).over(w7))
)

with_windows = (
    metrics_inventory
    .select(
        "id",
        "product_id",
        "quantity_filled",
        "last_restock_period",
        "price",
        "cost",
        "product_name",
        "product_cost",
        "product_id_avg_cost",
        "product_cost_rank",
        "seq_no",
        "quantity_filled_bucket",
        "first_known_cost",
        "running_max_product_cost_rank",
        "running_min_product_cost_rank",
        F.stddev_samp("seq_no").over(w).alias("product_id_stddev_samp_seq_no"),
        F.round((F.col("product_cost") / F.sum("product_cost").over(w)), 4).alias("product_cost_share"),
    )
)

result = (
    with_windows
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .groupingSets(
        [["brand", "category"], ["brand"], []],
        F.col("p.brand").alias("brand"),
        F.col("p.category").alias("category"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("e.price")).alias("total_price"),
    )
)
