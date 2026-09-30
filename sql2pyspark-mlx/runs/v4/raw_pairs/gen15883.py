from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id").orderBy(F.col("running_revenue").asc_nulls_first(), "id")
w4 = Window.partitionBy("store_id", "product_id").orderBy("revenue", "id").rowsBetween(-3, Window.currentRow)
w_running = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id")
w8 = Window.partitionBy("product_id").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("product_id", "store_id").orderBy("units_share_avg_3", "id")
w10 = Window.partitionBy("product_id").orderBy("running_revenue", "id")

enriched = (
    df_store_sales
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.product_name"))
)

with_windows = (
    enriched
    .select(
        "*",
        F.sum("revenue").over(w).alias("running_revenue"),
        F.round((F.col("units") / F.sum("units").over(w2)), 4).alias("units_share"),
    )
)

latest = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    latest
    .select(
        "*",
        F.avg("units_share").over(w4).alias("units_share_avg_3"),
        F.sum("revenue").over(w_running).alias("running_revenue_2"),
        F.first("revenue", ignorenulls=True).over(w6).alias("first_known_revenue"),
        F.avg("units").over(w7).alias("product_id_avg_units"),
        F.sum("running_revenue").over(w8).alias("cumulative_running_revenue"),
    )
)

cleaned = (
    scored
    .filter(F.col("cumulative_running_revenue") >= 57969)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "product_name",
        "running_revenue",
        "units_share",
        "units_share_avg_3",
        "first_known_revenue",
        "product_id_avg_units",
        "cumulative_running_revenue",
        F.when(F.col("units_share_avg_3") >= 1, "High").when(F.col("units_share_avg_3") >= 0, "Medium").otherwise("Low").alias("units_share_avg_3_band"),
    )
)

metrics = (
    cleaned
    .withColumn("prev_units_share", F.lag("units_share", 1).over(w9))
    .withColumn("corr_units_revenue", F.corr("units", "revenue").over(w7))
    .withColumn("next_revenue", F.lead("revenue", 1).over(w10))
)

cleaned_store_sales = (
    metrics
    .filter(F.col("sale_date") > F.lit("2023-07-30").cast("date"))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        "product_name",
        "running_revenue",
        "units_share_avg_3",
        "first_known_revenue",
        "product_id_avg_units",
        "units_share_avg_3_band",
        "prev_units_share",
        "corr_units_revenue",
        F.coalesce("next_revenue", F.lit(0)).alias("next_revenue_filled"),
        F.length("product_name").alias("product_name_length"),
    )
)

result = (
    cleaned_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "product_name",
        "units_share_avg_3",
        "first_known_revenue",
        "product_id_avg_units",
        "prev_units_share",
    )
)
