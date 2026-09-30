from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part = Window.partitionBy("product_id", "store_id")
w3 = Window.partitionBy("product_id")
w_running = Window.partitionBy("product_id").orderBy("product_id_stddev_samp_units_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("units_share", "id")
w6 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy(F.col("units").desc_nulls_first())
w_ordered = Window.partitionBy("product_id").orderBy("running_min_revenue", "id").rowsBetween(-5, -1)
w9 = Window.partitionBy("product_id").orderBy("last4_sum_running_count_revenue")
w_ordered_2 = Window.partitionBy("product_id").orderBy("last4_min_running_count_revenue", "id").rowsBetween(-3, Window.currentRow)
w11 = Window.partitionBy("product_id").orderBy("units", "id")

cleaned = (
    df_store_sales
    .filter(F.col("units") == 22)
    .select("*", F.coalesce("units", F.lit(0)).alias("units_filled"))
)

scored = (
    cleaned
    .withColumn("running_revenue", F.sum("revenue").over(w_hist))
    .withColumn("product_id_stddev_samp_units_filled", F.stddev_samp("units_filled").over(w_part))
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w3)), 4))
)

metrics = (
    scored
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "units_filled",
        "product_id_stddev_samp_units_filled",
        "units_share",
        F.sum("revenue").over(w_running).alias("running_sum_revenue"),
        F.min("revenue").over(w_running).alias("running_min_revenue"),
        F.count("revenue").over(w_running).alias("running_count_revenue"),
        F.lag("units_filled", 2).over(w5).alias("prev_units_filled"),
    )
)

scored_store_sales = (
    metrics
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "running_min_revenue",
        F.min("running_count_revenue").over(w6).alias("last4_min_running_count_revenue"),
        F.max("running_count_revenue").over(w6).alias("last4_max_running_count_revenue"),
        F.count("running_count_revenue").over(w6).alias("last4_count_running_count_revenue"),
        F.sum("running_count_revenue").over(w6).alias("last4_sum_running_count_revenue"),
    )
)

metrics_store_sales = (
    scored_store_sales
    .withColumn("units_percent_rank", F.percent_rank().over(w7))
    .withColumn("last4_min_running_count_revenue_avg_5_prior", F.avg("last4_min_running_count_revenue").over(w_ordered))
    .withColumn("last4_sum_running_count_revenue_rank", F.rank().over(w9))
    .withColumn("last4_min_running_count_revenue_avg_3", F.avg("last4_min_running_count_revenue").over(w_ordered_2))
    .withColumn("prev_last4_max_running_count_revenue", F.lag("last4_max_running_count_revenue", 1).over(w11))
)

result = (
    metrics_store_sales
    .select(
        "id",
        "product_id",
        "units",
        "running_min_revenue",
        "last4_min_running_count_revenue",
        "last4_min_running_count_revenue_avg_5_prior",
        "last4_min_running_count_revenue_avg_3",
        "prev_last4_max_running_count_revenue",
    )
    .orderBy("last4_min_running_count_revenue_avg_3")
)
