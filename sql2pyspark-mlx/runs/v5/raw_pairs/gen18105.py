from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("price").asc_nulls_first())
w2 = Window.partitionBy("store_id", "product_id")
w3 = Window.orderBy(F.col("price").desc_nulls_first())
w4 = Window.orderBy("price")
w5 = Window.partitionBy("product_id", "category")
w6 = Window.partitionBy("category", "product_id")
w7 = Window.orderBy(F.col("price_percent_rank").asc_nulls_first())
w_ordered = Window.partitionBy("category", "product_id").orderBy(F.col("price_percent_rank").desc())
amount_by_store_id = df_orders.groupBy("store_id").agg(F.sum("amount").alias("sum_order_amount"))

filtered = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-02-04").cast("date")) & F.col("store_id").isin([2, 3, 4]))
    .withColumn("last_restock_label", F.date_format("last_restock", "yyyy-MM-dd"))
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 1))
)

joined = (
    filtered
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.last_restock",
        "src.last_restock_label",
        "src.last_restock_plus_months",
        F.col("d.category"),
        F.col("d.product_name"),
        F.col("d.price"),
    )
)

remaining_keys = (
    joined
    .select("id")
    .subtract(
        df_inventory
        .filter(F.col("store_id").isin([1, 2, 3]))
        .select("id")
    )
)

verified = (
    joined
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = (
    verified
    .filter(F.col("store_id").isin([2, 3, 5]))
    .select(
        "*",
        F.dense_rank().over(w).alias("price_dense_rank"),
        F.count("price").over(w2).alias("store_id_count_price"),
        F.dense_rank().over(w3).alias("price_dense_rank_2"),
        F.percent_rank().over(w4).alias("price_percent_rank"),
        F.count("price").over(w5).alias("product_id_count_price"),
    )
)

metrics = (
    with_windows
    .select(
        "store_id",
        "product_id",
        "last_restock",
        "last_restock_label",
        "category",
        "product_name",
        "price",
        "store_id_count_price",
        "price_dense_rank_2",
        "price_percent_rank",
        "product_id_count_price",
        F.max("price_dense_rank").over(w6).alias("category_max_price_dense_rank"),
        F.rank().over(w7).alias("price_percent_rank_rank"),
        F.dense_rank().over(w_ordered).alias("price_percent_rank_dense_rank"),
    )
)

result = (
    metrics
    .join(amount_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "product_id",
        "last_restock_label",
        "category",
        "product_name",
        "price",
        "store_id_count_price",
        "product_id_count_price",
        "price_percent_rank_rank",
        F.when(F.col("product_name").rlike("^(Yoga Mat|Office Chair)$"), "CORE").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("product_name_zone"),
        F.col("sum_order_amount"),
    )
    .orderBy(F.col("product_id").desc())
)
