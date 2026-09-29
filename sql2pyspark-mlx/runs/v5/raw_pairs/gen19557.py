from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("brand")
w2 = Window.partitionBy("brand").orderBy(F.col("launch_date").desc(), "id")
w3 = Window.partitionBy("supplier_id", "category").orderBy("price_filled", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.orderBy("launch_date", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("category").orderBy(F.col("price_avg_2").desc_nulls_first())
w6 = Window.partitionBy("brand").orderBy("price_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("category").orderBy("price_avg_6", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("category", "brand")
w9 = Window.orderBy("brand_max_price_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("price_avg_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("brand").orderBy("running_row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w12 = Window.partitionBy("brand").orderBy(F.col("cumulative_row_seq").desc_nulls_last())
w13 = Window.partitionBy("brand").orderBy("price_avg_6", "id")
w14 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

valid = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-03-08").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "launch_date",
        "tags",
        "supplier_id",
        F.coalesce("price", F.lit(0)).alias("price_filled"),
    )
)

scored = (
    valid
    .filter(F.month("launch_date").isin([3, 10, 11]))
    .select(
        "id",
        "category",
        "brand",
        "tags",
        "price_filled",
        F.max("price_filled").over(w_ordered).alias("brand_max_price_filled"),
        F.row_number().over(w2).alias("row_seq"),
        F.avg("price").over(w3).alias("price_avg_2"),
        F.avg("price").over(w4).alias("price_avg_6"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.rank().over(w5).alias("price_avg_2_rank"),
        F.sum("row_seq").over(w6).alias("running_row_seq"),
        F.avg("price_avg_2").over(w7).alias("price_avg_2_avg_3"),
        F.min("price_avg_6").over(w8).alias("category_min_price_avg_6"),
        F.sum("row_seq").over(w9).alias("cumulative_row_seq"),
    )
)

base = (
    enriched
    .filter(~F.col("brand").eqNullSafe(F.lit("Globex")))
    .select(
        "id",
        "brand",
        "price_filled",
        "brand_max_price_filled",
        "row_seq",
        "price_avg_2",
        "price_avg_6",
        "running_row_seq",
        "price_avg_2_avg_3",
        "cumulative_row_seq",
        (F.col("cumulative_row_seq") - F.col("price_avg_2_rank")).alias("cumulative_row_seq_minus_price_avg_2_rank"),
        F.coalesce("brand", F.lit("DIRECT")).alias("resolved_brand"),
    )
)

metrics = (
    base
    .withColumn("cumulative_running_row_seq", F.sum("running_row_seq").over(w10))
    .withColumn("last_known_cumulative_row_seq_minus_price_avg_2_rank", F.last("cumulative_row_seq_minus_price_avg_2_rank", ignorenulls=True).over(w11))
    .withColumn("brand_stddev_samp_running_row_seq", F.stddev_samp("running_row_seq").over(w_ordered))
    .withColumn("cumulative_row_seq_cume_dist", F.cume_dist().over(w12))
    .withColumn("price_avg_6_bucket", F.ntile(3).over(w13))
)

with_last = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w14))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand",
        "s.price_filled",
        "s.price_avg_6",
        "s.running_row_seq",
        "s.cumulative_row_seq",
        "s.resolved_brand",
        "s.last_known_cumulative_row_seq_minus_price_avg_2_rank",
        "s.brand_stddev_samp_running_row_seq",
        F.col("c.revenue"),
    )
)

result = (
    with_last
    .select(
        "id",
        "brand",
        "price_avg_6",
        "running_row_seq",
        "cumulative_row_seq",
        "resolved_brand",
        "brand_stddev_samp_running_row_seq",
        "revenue",
    )
    .orderBy("brand_stddev_samp_running_row_seq")
)
