from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id")
w2 = Window.orderBy(F.col("revenue_int").desc_nulls_first())
w3 = Window.partitionBy("product_id")
w_grp = Window.partitionBy("store_id")
w5 = Window.partitionBy("product_id").orderBy(F.col("revenue_int").desc_nulls_first())
w6 = Window.partitionBy("product_id", "store_id").orderBy(F.col("store_id_min_revenue_int").desc_nulls_first())
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.max("quantity").alias("max_inventory_quantity"))

base = (
    df_store_sales
    .filter(F.col("store_id") == 3)
    .select(
        "*",
        F.col("revenue").cast("int").alias("revenue_int"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

matched_keys = (
    base
    .select("id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date").between("2023-06-12", "2024-07-20"))
        .select("id", "store_id")
    )
)

reconciled = (
    base
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "*",
        F.min("revenue_int").over(w).alias("store_id_min_revenue_int"),
        F.dense_rank().over(w2).alias("revenue_int_dense_rank"),
        F.sum("units").over(w3).alias("product_id_sum_units"),
    )
)

with_windows_store_sales = (
    with_windows
    .withColumn("store_id_sum_revenue", F.sum("revenue").over(w_grp))
    .withColumn("revenue_int_dense_rank_2", F.dense_rank().over(w5))
)

metrics = (
    with_windows_store_sales
    .select(
        "*",
        F.round((F.col("store_id_sum_revenue") / F.sum("store_id_sum_revenue").over(w_grp)), 4).alias("store_id_sum_revenue_share"),
        F.dense_rank().over(w6).alias("store_id_min_revenue_int_dense_rank"),
    )
)

enriched = (
    metrics
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.cost"), F.col("b.category"))
)

result = (
    enriched
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        "revenue_int",
        "sale_date_month_end",
        "store_id_min_revenue_int",
        "revenue_int_dense_rank",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("store_id").asc_nulls_first(), F.col("units").desc())
)
