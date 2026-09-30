from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("sale_date").asc_nulls_last(), "id")
w_ordered = Window.partitionBy("store_id")
w_ordered_2 = Window.partitionBy("store_id").orderBy(F.col("revenue_per_unit").desc(), "id")
w4 = Window.partitionBy("store_id").orderBy("store_id_min_revenue", "id")
w5 = Window.partitionBy("store_id").orderBy(F.col("revenue_per_unit").desc_nulls_first(), "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("revenue_per_unit").desc(), "id")
w7 = Window.partitionBy("store_id").orderBy("sale_date", "id")
w8 = Window.partitionBy("product_id").orderBy("store_id_min_revenue", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.partitionBy("product_id").orderBy(F.col("revenue_stddev").desc())

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-03-20").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("units") * F.col("revenue")).alias("revenue_per_unit"),
    )
)

ranked = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    ranked
    .withColumn("store_id_min_revenue", F.min("revenue").over(w_ordered))
    .withColumn("revenue_per_unit_bucket", F.ntile(3).over(w_ordered_2))
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

base = (
    deduped
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-06-25")))
    .select("*", F.month("sale_date").alias("sale_date_month"))
)

deduped_store_sales = (
    base
    .withColumn("row_num_2", F.row_number().over(w5))
    .filter(F.col("row_num_2") == 1)
)

scored = (
    deduped_store_sales
    .select(
        "*",
        F.ntile(10).over(w6).alias("revenue_per_unit_bucket_2"),
        F.lead("revenue", 2).over(w7).alias("next_revenue"),
        F.round((F.col("units") / F.sum("units").over(w_ordered)), 4).alias("units_share"),
        F.avg("store_id_min_revenue").over(w8).alias("store_id_min_revenue_avg_2"),
    )
)

result = (
    scored
    .groupBy(
        "product_id",
        F.when(F.col("revenue") >= 3100, "PREMIUM").otherwise("STANDARD").alias("revenue_tier"),
        "store_id",
    )
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.round(F.avg("units_share"), 2).alias("avg_units_share"),
        F.corr("units", "revenue").alias("corr_units_revenue"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
    )
    .withColumn("revenue_stddev_rank", F.rank().over(w9))
    .orderBy(F.col("distinct_store_ids").desc())
)
