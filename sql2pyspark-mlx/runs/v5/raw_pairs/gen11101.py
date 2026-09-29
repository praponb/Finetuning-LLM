from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("store_id", "product_id").orderBy("revenue_per_unit", "id")
w2 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("units", "id")
w_ordered = Window.partitionBy("product_id").orderBy("cumulative_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id").orderBy("event_seq", "id")
w6 = Window.partitionBy("store_id")
w7 = Window.partitionBy("store_id").orderBy("cumulative_units", "id").rowsBetween(-5, -1)

base = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-04-11").cast("date")) & F.col("store_id").isin([4, 6]))
    .withColumn("revenue_per_unit", F.round((F.col("units") / F.col("revenue")), 2))
)

enriched = (
    base
    .filter(F.month("sale_date").isin([1, 2, 6]))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("cumulative_units", F.sum("units").over(w2))
)

enriched_store_sales = (
    enriched
    .select(
        "*",
        F.lag("cumulative_units").over(w3).alias("prev_cumulative_units"),
        F.lead("revenue").over(w3).alias("next_revenue"),
        F.lag("revenue").over(w3).alias("prev_revenue"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

metrics = (
    enriched_store_sales
    .select(
        "*",
        F.sum("prev_cumulative_units").over(w_ordered).alias("running_prev_cumulative_units"),
        F.row_number().over(w5).alias("event_seq_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "event_seq",
        "prev_cumulative_units",
        "prev_revenue",
        "event_seq_2",
        F.round((F.col("prev_revenue") / F.sum("prev_revenue").over(w6)), 4).alias("prev_revenue_share"),
        F.avg("running_prev_cumulative_units").over(w7).alias("running_prev_cumulative_units_avg_5_prior"),
    )
)
