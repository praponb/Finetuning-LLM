from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("store_id").orderBy("revenue", "id")
w2 = Window.orderBy("revenue", "id")
w3 = Window.partitionBy("store_id")
w4 = Window.orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("next_revenue_adjusted", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.orderBy("next_revenue_adjusted", "id")

valid = (
    df_store_sales
    .filter(F.col("product_id").isin([7, 8]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.round((F.col("revenue") * 100), 2).alias("revenue_adjusted"),
    )
)

metrics = (
    valid
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        F.lead("revenue_adjusted", 1).over(w_ordered).alias("next_revenue_adjusted"),
        F.lag("revenue_adjusted", 2).over(w2).alias("prev_revenue_adjusted"),
        F.sum("revenue_adjusted").over(w3).alias("store_id_sum_revenue_adjusted"),
    )
)

metrics_store_sales = (
    metrics
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        "next_revenue_adjusted",
        F.last("store_id_sum_revenue_adjusted", ignorenulls=True).over(w4).alias("last_known_store_id_sum_revenue_adjusted"),
        F.ntile(3).over(w2).alias("revenue_bucket"),
        F.sum("prev_revenue_adjusted").over(w5).alias("cumulative_prev_revenue_adjusted"),
    )
)

with_windows = (
    metrics_store_sales
    .filter(F.col("last_known_store_id_sum_revenue_adjusted") <= 151)
    .withColumn("next_revenue", F.lead("revenue").over(w_grp))
    .withColumn("next_revenue_bucket", F.lead("revenue_bucket").over(w_grp))
    .withColumn("prev_revenue_bucket", F.lag("revenue_bucket").over(w_grp))
    .withColumn("seq_no", F.row_number().over(w_grp))
)

matched_keys = (
    with_windows
    .select("id")
    .intersectAll(
        df_store_sales
        .filter(F.col("store_id") == 1)
        .select("id")
    )
)

reconciled = (
    with_windows
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

valid_store_sales = (
    reconciled
    .filter(F.col("sale_date") >= F.lit("2023-07-05").cast("date"))
    .withColumn("seq_no_str", F.col("seq_no").cast("double"))
)

valid_store_sales_2 = (
    valid_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-08-19"))) & F.col("next_revenue_bucket").between(3, 4))
    .select(
        "*",
        F.when(F.col("seq_no_str") >= 88, "High").when(F.col("seq_no_str") >= 20, "Medium").otherwise("Low").alias("seq_no_str_band"),
        F.when(F.col("last_known_store_id_sum_revenue_adjusted") >= 326, "High").when(F.col("last_known_store_id_sum_revenue_adjusted") >= 149, "Medium").otherwise("Low").alias("last_known_store_id_sum_revenue_adjusted_level"),
    )
)

grouped = (
    valid_store_sales_2
    .groupBy(
        F.when(F.col("revenue") >= 3500, "LARGE").otherwise("SMALL").alias("revenue_tier"),
        F.when(F.col("units") >= 37, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
        "seq_no",
    )
    .agg(
        F.max("cumulative_prev_revenue_adjusted").alias("max_cumulative_prev_revenue_adjusted"),
        F.min("revenue_bucket").alias("min_revenue_bucket"),
    )
    .filter(F.col("max_cumulative_prev_revenue_adjusted") >= 0)
)

result = (
    grouped
    .select(
        "revenue_tier",
        "units_tier",
        "seq_no",
        "max_cumulative_prev_revenue_adjusted",
        "min_revenue_bucket",
    )
    .orderBy("seq_no")
)
