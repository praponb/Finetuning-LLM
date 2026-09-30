from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.partitionBy("store_id", "product_id").orderBy("launch_date", "id").rowsBetween(-2, -1)
w_grp = Window.partitionBy("product_id")
w5 = Window.orderBy(F.col("running_min_units").desc(), "id")
w_part = Window.partitionBy("store_id")
w7 = Window.orderBy("running_sum_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_store_sales
    .filter(F.col("revenue").between(1000, 2200))
    .withColumn("units_rounded", F.round("units", 0))
)

metrics = (
    valid
    .filter(F.col("units_rounded") <= 74)
    .select(
        "*",
        F.sum("units").over(w).alias("running_sum_units"),
        F.count("units").over(w).alias("running_count_units"),
        F.min("units").over(w).alias("running_min_units"),
        F.round((F.col("revenue") / F.sum("revenue").over(w2)), 4).alias("revenue_share"),
    )
)

with_dims = (
    metrics
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.product_name"), F.col("b.launch_date"), F.col("b.price"))
)

enriched = (
    with_dims
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.sqft"), F.col("d.opened_date"))
)

enriched_store_sales = (
    enriched
    .select(
        "id",
        "sale_date",
        "units",
        "units_rounded",
        "running_sum_units",
        "running_min_units",
        "sqft",
        "opened_date",
        F.avg("units").over(w3).alias("units_avg_2_prior"),
        F.count("units_rounded").over(w_grp).alias("product_id_count_units_rounded"),
        F.ntile(10).over(w5).alias("running_min_units_bucket"),
        F.sum("price").over(w_part).alias("store_id_sum_price"),
        F.sum("units").over(w7).alias("cumulative_units"),
    )
)

base = (
    enriched_store_sales
    .filter(F.col("opened_date") >= "2023-10-01")
    .select("*", F.col("running_sum_units").cast("int").alias("running_sum_units_int"))
)

result = (
    base
    .groupBy(
        F.date_trunc("WEEK", "opened_date").alias("week_start"),
        F.when(F.col("cumulative_units") >= 207, "LARGE").otherwise("SMALL").alias("cumulative_units_tier"),
        F.date_trunc("MONTH", "sale_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("running_sum_units", [0.25, 0.5, 0.75]).alias("running_sum_units_quartiles"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("units_rounded").alias("min_units_rounded"),
    )
    .filter(F.col("min_units_rounded") > 0)
)
