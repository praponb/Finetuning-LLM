from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sale_date").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("store_id").orderBy(F.col("units_bucket_whole").desc(), "id")
w5 = Window.partitionBy("store_id").orderBy("corr_units_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy(F.col("corr_units_revenue").desc_nulls_first())
w7 = Window.partitionBy("store_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("units_bucket_whole_bucket", "id").rowsBetween(-2, -1)
w9 = Window.orderBy("seq_no", "id")
w10 = Window.orderBy(F.col("units_bucket").desc())
w11 = Window.orderBy("units_bucket_whole_bucket", "id")

with_dims = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.sqft"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.city"), F.col("b.store_name"), F.col("b.opened_date"))
)

scored = (
    enriched
    .select(
        "id",
        "store_id",
        "sale_date",
        "sqft",
        "opened_date",
        F.row_number().over(w).alias("seq_no"),
        F.ntile(5).over(w2).alias("units_bucket"),
        F.corr("units", "revenue").over(w3).alias("corr_units_revenue"),
    )
)

valid = (
    scored
    .filter(F.col("sale_date") > F.lit("2023-06-04").cast("date"))
    .withColumn("units_bucket_whole", F.ceil("units_bucket"))
)

metrics = (
    valid
    .select(
        "id",
        "sale_date",
        "seq_no",
        "units_bucket",
        F.ntile(3).over(w4).alias("units_bucket_whole_bucket"),
        F.first("sqft", ignorenulls=True).over(w5).alias("first_known_sqft"),
        F.cume_dist().over(w6).alias("corr_units_revenue_cume_dist"),
        F.last("units_bucket_whole", ignorenulls=True).over(w7).alias("last_known_units_bucket_whole"),
    )
)

filtered = (
    metrics
    .filter((F.col("sale_date") > F.lit("2023-09-24").cast("date")) & F.col("units_bucket").between(3, 7))
    .select(
        "id",
        "sale_date",
        "seq_no",
        "units_bucket",
        "units_bucket_whole_bucket",
        "first_known_sqft",
        (F.col("seq_no") * F.col("first_known_sqft")).alias("seq_no_times_first_known_sqft"),
    )
)

scored_store_sales = (
    filtered
    .withColumn("seq_no_times_first_known_sqft_avg_2_prior", F.avg("seq_no_times_first_known_sqft").over(w8))
    .withColumn("event_seq", F.row_number().over(w9))
    .withColumn("units_bucket_cume_dist", F.cume_dist().over(w10))
    .withColumn("units_bucket_whole_bucket_bucket", F.ntile(3).over(w11))
)

result = (
    scored_store_sales
    .groupBy(F.date_trunc("YEAR", "sale_date").alias("year_start"))
    .agg(
        F.percentile_approx("first_known_sqft", 0.95).alias("first_known_sqft_p95"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("year_start").desc_nulls_first())
)
