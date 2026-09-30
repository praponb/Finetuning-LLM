from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w_hist = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("product_id").orderBy("sale_date_day", "id")
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("store_id", "product_id")
w6 = Window.partitionBy("store_id").orderBy("units_bucket", "id")
w7 = Window.partitionBy("product_id", "store_id").orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy("units_bucket", "id").rowsBetween(-3, Window.currentRow)
w9 = Window.partitionBy("store_id", "product_id").orderBy("units", "id")
w_ordered = Window.partitionBy("product_id").orderBy("revenue", "id")

scored = (
    df_store_sales
    .select(
        "*",
        F.ntile(5).over(w_running).alias("units_bucket"),
        F.avg("units").over(w_hist).alias("units_avg_6_prior"),
    )
)

filtered = (
    scored
    .filter((F.col("sale_date") > F.lit("2023-07-31").cast("date")) & (F.col("sale_date") <= "2025-10-01"))
    .select("*", F.dayofmonth("sale_date").alias("sale_date_day"))
)

latest = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

enriched = (
    latest
    .withColumn("units_bucket_share", F.round((F.col("units_bucket") / F.sum("units_bucket").over(w4)), 4))
    .withColumn("store_id_stddev_samp_units", F.stddev_samp("units").over(w5))
    .withColumn("units_bucket_bucket", F.ntile(10).over(w6))
    .withColumn("running_units_avg_6_prior", F.sum("units_avg_6_prior").over(w7))
    .withColumn("units_bucket_avg_3", F.avg("units_bucket").over(w8))
)

enriched_store_sales = (
    enriched
    .filter(F.col("store_id_stddev_samp_units") != 37)
    .withColumn("prev_units_bucket_bucket", F.lag("units_bucket_bucket").over(w9))
    .withColumn("prev_sale_date_day", F.lag("sale_date_day").over(w9))
    .withColumn("next_sale_date_day", F.lead("sale_date_day").over(w9))
    .withColumn("event_seq", F.row_number().over(w9))
    .withColumn("row_seq", F.row_number().over(w_ordered))
)

result = (
    enriched_store_sales
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "units_bucket",
        "units_avg_6_prior",
        "sale_date_day",
        "units_bucket_share",
    )
    .orderBy(F.col("product_id").desc())
)
