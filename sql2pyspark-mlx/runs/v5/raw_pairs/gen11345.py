from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("product_id", "store_id").orderBy("units", "id")
w_running = Window.partitionBy("store_id", "product_id").orderBy("revenue_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("units")
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("store_id").orderBy("corr_units_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("first_known_revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.max("shipping_fee").alias("max_order_shipping_fee"))

valid = (
    df_store_sales
    .filter(F.col("store_id") == 4)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit"),
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)

scored = (
    valid
    .select(
        "id",
        "store_id",
        "revenue",
        "revenue_per_unit",
        F.ntile(4).over(w_part).alias("units_bucket"),
        F.first("revenue_per_unit", ignorenulls=True).over(w_running).alias("first_known_revenue_per_unit"),
        F.dense_rank().over(w3).alias("units_dense_rank"),
        F.corr("units", "revenue").over(w4).alias("corr_units_revenue"),
    )
)

enriched = (
    scored
    .withColumn("last_known_units_bucket", F.last("units_bucket", ignorenulls=True).over(w5))
    .withColumn("first_known_revenue", F.first("revenue", ignorenulls=True).over(w6))
)

matched_keys = (
    enriched
    .select("store_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("units") != 30)
        .select("store_id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter(F.col("corr_units_revenue").isNotNull())
    .withColumn("revenue_per_unit_rounded", F.round("revenue_per_unit", 2))
)

result = (
    filtered
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "revenue",
        "revenue_per_unit",
        "units_dense_rank",
        "corr_units_revenue",
        "last_known_units_bucket",
        "first_known_revenue",
        "revenue_per_unit_rounded",
        F.col("max_order_shipping_fee"),
    )
    .orderBy(F.col("revenue_per_unit_rounded").desc(), "corr_units_revenue")
)
