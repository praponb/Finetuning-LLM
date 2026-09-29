from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue_adjusted").desc_nulls_last())
w2 = Window.partitionBy("product_id").orderBy("units", "id")
w3 = Window.partitionBy("region").orderBy(F.col("revenue").asc_nulls_first(), "id")
w4 = Window.partitionBy("product_id", "store_id").orderBy(F.col("sale_date_month").asc_nulls_last(), "id")
w_hist = Window.partitionBy("region").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id").orderBy(F.col("next_sale_date_month").desc_nulls_last())
w7 = Window.orderBy(F.col("sale_date").desc(), "id")
w8 = Window.partitionBy("product_id", "store_id").orderBy("sale_date_month", "id")
w9 = Window.orderBy("units", "id")
w10 = Window.partitionBy("store_id").orderBy(F.col("sale_date_month").desc_nulls_last())
w11 = Window.partitionBy("store_id").orderBy("revenue_adjusted_rank", "id").rowsBetween(-5, Window.currentRow)

joined = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.region"), F.col("d.store_name"), F.col("d.opened_date"))
)

base = (
    joined
    .filter(F.col("region").isNotNull())
    .withColumn("revenue_adjusted", F.round((F.col("revenue") * 0.9), 2))
    .withColumn("sale_date_month", F.month("sale_date"))
    .withColumn("resolved_region", F.coalesce("region", F.lit("OTHER")))
)

metrics = (
    base
    .withColumn("revenue_adjusted_rank", F.rank().over(w))
    .withColumn("next_sale_date_month", F.lead("sale_date_month", 1).over(w2))
)

latest = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

latest_store_sales = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest_store_sales
    .select(
        "*",
        F.last("next_sale_date_month", ignorenulls=True).over(w_hist).alias("last_known_next_sale_date_month"),
        F.dense_rank().over(w6).alias("next_sale_date_month_dense_rank"),
    )
)

enriched = (
    with_windows
    .withColumn("event_seq", F.row_number().over(w7))
    .withColumn("row_seq", F.row_number().over(w8))
    .withColumn("next_last_known_next_sale_date_month", F.lead("last_known_next_sale_date_month", 1, 0).over(w9))
    .withColumn("sale_date_month_cume_dist", F.cume_dist().over(w10))
    .withColumn("revenue_adjusted_avg_5", F.avg("revenue_adjusted").over(w11))
)

result = (
    enriched
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "region",
        "opened_date",
        "revenue_adjusted",
        "sale_date_month",
    )
    .orderBy(F.col("product_id").desc(), F.col("sale_date_month").desc())
)
