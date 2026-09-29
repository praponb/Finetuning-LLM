from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy("sale_date", "id")
w3 = Window.partitionBy("product_id")
w4 = Window.partitionBy("product_id").orderBy(F.col("revenue").desc_nulls_last())
w5 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy("store_id").orderBy(F.col("revenue_share").desc(), "id")
w7 = Window.orderBy(F.col("revenue_percent_rank").desc_nulls_last())
w_grp = Window.orderBy("revenue", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue_share_2").desc())

with_windows = (
    df_store_sales
    .select(
        "*",
        F.round((F.col("revenue") / F.sum("revenue").over(w)), 4).alias("revenue_share"),
        F.lead("revenue", 1, 0).over(w2).alias("next_revenue"),
        F.round((F.col("revenue") / F.sum("revenue").over(w3)), 4).alias("revenue_share_2"),
        F.percent_rank().over(w4).alias("revenue_percent_rank"),
    )
)

joined = (
    with_windows
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.store_name"))
)

with_windows_store_sales = (
    joined
    .withColumn("next_revenue_avg_5_prior", F.avg("next_revenue").over(w5))
    .withColumn("revenue_share_bucket", F.ntile(3).over(w6))
    .withColumn("revenue_percent_rank_rank", F.rank().over(w7))
    .withColumn("next_revenue_share", F.round((F.col("next_revenue") / F.sum("next_revenue").over(w3)), 4))
    .withColumn("revenue_percent_rank_avg_6", F.avg("revenue_percent_rank").over(w_grp))
)

deduped = (
    with_windows_store_sales
    .withColumn("latest_rank", F.rank().over(w9))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

grouped = (
    deduped
    .withColumn("revenue_share_tier", F.when(F.col("revenue_share") >= 0.75, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["revenue_share_tier", "store_id"], ["revenue_share_tier"], []],
        "revenue_share_tier",
        "store_id",
    )
    .agg(
        F.round(F.avg("revenue_share"), 2).alias("avg_revenue_share"),
        F.round(F.avg("revenue_percent_rank_avg_6"), 2).alias("avg_revenue_percent_rank_avg_6"),
        F.min("revenue_share_bucket").alias("min_revenue_share_bucket"),
    )
)

result = (
    grouped
    .select(
        "revenue_share_tier",
        "store_id",
        "avg_revenue_share",
        "min_revenue_share_bucket",
    )
    .orderBy(F.col("min_revenue_share_bucket").desc())
)
