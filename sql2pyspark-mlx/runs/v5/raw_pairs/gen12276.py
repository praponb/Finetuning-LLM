from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_first())
w4 = Window.partitionBy("store_id").orderBy("units_str", "id")
w5 = Window.orderBy("units", "id").rowsBetween(-2, Window.currentRow)
w_recent = Window.partitionBy("store_id", "product_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("store_id").orderBy("revenue_share_2")
w8 = Window.partitionBy("product_id").orderBy("running_count_event_seq")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.sum("quantity").alias("sum_order_item_quantity"))

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-10-06").cast("date")) & (F.col("revenue") <= 1700))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.col("units").cast("double").alias("units_str"),
    )
)

enriched = (
    cleaned
    .filter(F.col("revenue") >= 3800)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units_str",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 4).alias("revenue_share"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share_2"),
        F.dense_rank().over(w3).alias("units_dense_rank"),
        F.row_number().over(w4).alias("event_seq"),
        F.avg("units").over(w5).alias("units_avg_2"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "product_id",
        "revenue_share_2",
        "event_seq",
        "units_avg_2",
        F.sum("event_seq").over(w_recent).alias("running_sum_event_seq"),
        F.avg("event_seq").over(w_recent).alias("running_avg_event_seq"),
        F.count("event_seq").over(w_recent).alias("running_count_event_seq"),
        F.rank().over(w7).alias("revenue_share_2_rank"),
    )
)

joined = (
    with_windows
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.product_name"))
)

latest = (
    joined
    .withColumn("dedup_rank", F.rank().over(w8))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    latest
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "revenue_share_2",
        "event_seq",
        "units_avg_2",
        "running_sum_event_seq",
        "running_avg_event_seq",
        "revenue_share_2_rank",
        F.col("sum_order_item_quantity"),
    )
    .orderBy(F.col("revenue_share_2").desc())
)
