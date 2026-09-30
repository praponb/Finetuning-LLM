from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w2 = Window.orderBy("units", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy("store_id").orderBy("revenue_avg_3_prior", "id")
w4 = Window.partitionBy("store_id")
w5 = Window.orderBy("revenue", "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("units_share").desc_nulls_last(), "id")
w7 = Window.partitionBy("store_id", "product_id").orderBy(F.col("next_units").desc_nulls_first(), "id")

enriched = (
    df_store_sales
    .select(
        "*",
        F.avg("units").over(w).alias("product_id_avg_units"),
        F.avg("revenue").over(w2).alias("revenue_avg_3_prior"),
    )
)

metrics = (
    enriched
    .withColumn("next_units", F.lead("units", 1, 0).over(w3))
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w4)), 4))
    .withColumn("revenue_bucket", F.ntile(10).over(w5))
    .withColumn("product_id_count_revenue_avg_3_prior", F.count("revenue_avg_3_prior").over(w))
    .withColumn("units_share_2", F.round((F.col("units") / F.sum("units").over(w4)), 4))
)

latest = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w6))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    latest
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.launch_date"))
)

latest_store_sales = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

summary = (
    latest_store_sales
    .cube(
        F.when(F.col("product_id_count_revenue_avg_3_prior") >= 13, "HIGH").otherwise("LOW").alias("product_id_count_revenue_avg_3_prior_tier"),
        F.when(F.col("revenue_avg_3_prior") >= 3348, "LARGE").otherwise("SMALL").alias("revenue_avg_3_prior_tier"),
    )
    .agg(
        F.percentile_approx("next_units", [0.25, 0.5, 0.75]).alias("next_units_quartiles"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
    )
)

result = (
    summary
    .select(
        "product_id_count_revenue_avg_3_prior_tier",
        "revenue_avg_3_prior_tier",
        "next_units_quartiles",
        "corr_revenue_units",
    )
)
