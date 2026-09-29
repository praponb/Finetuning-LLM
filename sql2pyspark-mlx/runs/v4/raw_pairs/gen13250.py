from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("greatest_units_revenue", "id")
w_running = Window.partitionBy("store_id", "product_id").orderBy(F.col("greatest_units_revenue").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy(F.col("revenue").desc_nulls_first())
w5 = Window.orderBy("next_units", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("product_id")
w7 = Window.partitionBy("product_id").orderBy("cumulative_greatest_units_revenue", "id")
w8 = Window.partitionBy("product_id", "store_id").orderBy("greatest_units_revenue_bucket", "id")
w9 = Window.orderBy("dedup_rank", "id").rowsBetween(-5, -1)

valid = (
    df_store_sales
    .filter(F.col("revenue") > 3300)
    .withColumn("greatest_units_revenue", F.greatest("units", "revenue"))
)

enriched = (
    valid
    .withColumn("next_units", F.lead("units", 1).over(w))
    .withColumn("greatest_units_revenue_bucket", F.ntile(5).over(w_running))
    .withColumn("greatest_units_revenue_bucket_2", F.ntile(3).over(w_running))
    .withColumn("cumulative_greatest_units_revenue", F.sum("greatest_units_revenue").over(w3))
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    deduped
    .withColumn("greatest_units_revenue_bucket_avg_2_prior", F.avg("greatest_units_revenue_bucket").over(w5))
    .withColumn("product_id_count_cumulative_greatest_units_revenue", F.count("cumulative_greatest_units_revenue").over(w6))
)

enriched_store_sales = (
    metrics
    .select(
        "*",
        F.row_number().over(w7).alias("seq_no"),
        F.row_number().over(w8).alias("event_seq"),
        F.avg("revenue").over(w9).alias("revenue_avg_5_prior"),
    )
)

filtered = (
    enriched_store_sales
    .filter(F.col("sale_date") > F.lit("2023-10-07").cast("date"))
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "greatest_units_revenue",
        "greatest_units_revenue_bucket",
        "greatest_units_revenue_bucket_2",
        "greatest_units_revenue_bucket_avg_2_prior",
        "product_id_count_cumulative_greatest_units_revenue",
        "event_seq",
        "revenue_avg_5_prior",
        (F.col("seq_no") * F.col("units")).alias("seq_no_times_units"),
    )
)

result = (
    filtered
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "greatest_units_revenue",
        "greatest_units_revenue_bucket_2",
        "product_id_count_cumulative_greatest_units_revenue",
        "event_seq",
        "revenue_avg_5_prior",
    )
)
