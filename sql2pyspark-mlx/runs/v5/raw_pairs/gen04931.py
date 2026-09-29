from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("product_id").orderBy("revenue", "id")
w3 = Window.partitionBy("product_id", "store_id")
w_running = Window.orderBy("units", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("units", "id").rowsBetween(-2, -1)
w_recent = Window.orderBy("corr_revenue_units", "id")
w_running_2 = Window.orderBy("units_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("units", "id")
w9 = Window.orderBy(F.col("units").desc(), "id")

enriched = (
    df_store_sales
    .filter(F.col("sale_date") > "2025-08-01")
    .select(
        "id",
        "units",
        F.avg("revenue").over(w).alias("revenue_avg_6_prior"),
        F.lag("revenue", 2).over(w2).alias("prev_revenue"),
        F.corr("revenue", "units").over(w3).alias("corr_revenue_units"),
    )
)

filtered = (
    enriched
    .filter(F.col("revenue_avg_6_prior") < 1807)
    .select(
        "id",
        "units",
        "revenue_avg_6_prior",
        "prev_revenue",
        "corr_revenue_units",
        F.col("units").cast("double").alias("units_num"),
    )
)

enriched_store_sales = (
    filtered
    .select(
        "id",
        "units",
        "revenue_avg_6_prior",
        "units_num",
        F.avg("units_num").over(w_running).alias("units_num_avg_6_prior"),
        F.avg("prev_revenue").over(w5).alias("prev_revenue_avg_2_prior"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.sum("revenue_avg_6_prior").over(w_running_2).alias("running_revenue_avg_6_prior"),
    )
)

base = (
    enriched_store_sales
    .filter(F.col("units_num") < 49)
    .select(
        "id",
        "units",
        "units_num",
        "prev_revenue_avg_2_prior",
        "seq_no",
        "running_revenue_avg_6_prior",
        F.least("prev_revenue_avg_2_prior", "units").alias("least_prev_revenue_avg_2_prior_units"),
    )
)

scored = (
    base
    .withColumn("prev_units", F.lag("units", 1).over(w8))
    .withColumn("units_bucket", F.ntile(5).over(w9))
)

base_store_sales = (
    scored
    .filter(F.col("units_bucket") == 7)
    .select(
        "id",
        "units",
        "units_num",
        "prev_revenue_avg_2_prior",
        "running_revenue_avg_6_prior",
        "least_prev_revenue_avg_2_prior_units",
        "prev_units",
        F.round((F.col("units_bucket") / F.col("running_revenue_avg_6_prior")), 2).alias("units_bucket_per_running_revenue_avg_6_prior"),
    )
)

result = (
    base_store_sales
    .select(
        "id",
        "units",
        "prev_revenue_avg_2_prior",
        "running_revenue_avg_6_prior",
        "least_prev_revenue_avg_2_prior_units",
        "units_bucket_per_running_revenue_avg_6_prior",
    )
    .orderBy("id")
)
