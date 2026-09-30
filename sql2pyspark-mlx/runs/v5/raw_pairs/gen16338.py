from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue").desc_nulls_last())
w2 = Window.orderBy("sale_date", "id")
w_ordered = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("sale_date_plus_78d").desc(), "id")
w5 = Window.orderBy("revenue", "id").rowsBetween(-3, -1)
w6 = Window.orderBy(F.col("seq_no").desc(), "id")
w7 = Window.orderBy("cumulative_revenue", "id")
w8 = Window.orderBy(F.col("revenue").desc())
w9 = Window.orderBy(F.col("revenue_avg_3_prior").desc_nulls_last())

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-05-13").cast("date"))
    .select(
        "id",
        "sale_date",
        "revenue",
        F.when(F.col("revenue") >= 3200, "High").when(F.col("revenue") >= 900, "Medium").otherwise("Low").alias("revenue_level"),
        F.date_add("sale_date", 78).alias("sale_date_plus_78d"),
    )
)

metrics = (
    valid
    .withColumn("revenue_dense_rank", F.dense_rank().over(w))
    .withColumn("next_revenue", F.lead("revenue", 2).over(w2))
    .withColumn("cumulative_revenue", F.sum("revenue").over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w4))
    .withColumn("revenue_avg_3_prior", F.avg("revenue").over(w5))
)

enriched = (
    metrics
    .withColumn("seq_no_bucket", F.ntile(5).over(w6))
    .withColumn("cumulative_revenue_bucket", F.ntile(10).over(w7))
)

valid_store_sales = (
    enriched
    .filter(F.col("seq_no_bucket") == 5)
    .select(
        "id",
        "sale_date",
        "revenue",
        "revenue_level",
        "revenue_dense_rank",
        "next_revenue",
        "seq_no",
        "revenue_avg_3_prior",
        "seq_no_bucket",
        "cumulative_revenue_bucket",
        F.coalesce("cumulative_revenue", F.lit(0)).alias("cumulative_revenue_filled"),
    )
)

filtered = (
    valid_store_sales
    .filter(F.col("sale_date") > F.lit("2023-05-17").cast("date"))
    .select(
        "id",
        "sale_date",
        "revenue",
        "revenue_level",
        "revenue_dense_rank",
        "next_revenue",
        "seq_no",
        "revenue_avg_3_prior",
        "seq_no_bucket",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.round("revenue_dense_rank", 2).alias("revenue_dense_rank_rounded"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .intersect(
        df_store_sales
        .filter(F.col("product_id").isin([1, 4, 7, 9]))
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid_store_sales_2 = (
    kept_rows
    .filter(F.col("sale_date") >= F.lit("2023-05-23").cast("date"))
    .withColumn("sale_date_period", F.date_trunc("year", "sale_date"))
)

result = (
    valid_store_sales_2
    .select(
        "id",
        "sale_date",
        "revenue",
        "next_revenue",
        "revenue_avg_3_prior",
        F.cume_dist().over(w8).alias("revenue_cume_dist"),
        F.percent_rank().over(w9).alias("revenue_avg_3_prior_percent_rank"),
    )
)
