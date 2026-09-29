from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("sqft").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w5 = Window.orderBy("discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("adjusted_store_sale_revenue", "id")

enriched = (
    df_stores
    .filter(F.col("city").isin(["Bangkok", "Singapore"]))
    .select(
        "id",
        "sqft",
        F.sum("sqft").over(w).alias("cumulative_sqft"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

with_latest = (
    enriched
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.sqft", F.col("c.revenue").alias("store_sale_revenue"))
)

valid = (
    with_latest
    .filter(F.col("store_sale_revenue").isNotNull())
    .select(
        "*",
        F.round((F.col("store_sale_revenue") * 0.9), 2).alias("adjusted_store_sale_revenue"),
        F.when(F.col("store_sale_revenue") >= 3400, "High").when(F.col("store_sale_revenue") >= 1200, "Medium").otherwise("Low").alias("store_sale_revenue_band"),
    )
)

kept = (
    valid
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.discount") < 0.27), "left_anti")
)

kept_stores = (
    kept
    .alias("s")
    .join(df_orders.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.store_id").isin([10, 22])), "left_anti")
)

with_latest_stores = (
    kept_stores
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.sqft",
        "s.adjusted_store_sale_revenue",
        "s.store_sale_revenue_band",
        F.col("c.revenue").alias("store_sale_revenue_2"),
    )
)

latest_match = (
    with_latest_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.discount"))
)

result = (
    latest_match
    .select(
        "id",
        "adjusted_store_sale_revenue",
        "store_sale_revenue_2",
        F.sum("store_sale_revenue_2").over(w5).alias("running_store_sale_revenue_2"),
        F.ntile(5).over(w6).alias("adjusted_store_sale_revenue_bucket"),
    )
)
