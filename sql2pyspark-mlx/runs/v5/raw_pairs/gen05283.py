from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("units_filled", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy(F.col("units").desc())
w4 = Window.partitionBy("product_id", "store_id").orderBy("revenue", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units_filled").asc_nulls_first())
w6 = Window.orderBy("units_cume_dist", "id")
w7 = Window.partitionBy("store_id").orderBy("revenue", "id")
w8 = Window.orderBy("units_filled_percent_rank", "id").rowsBetween(-5, Window.currentRow)
w9 = Window.orderBy("revenue_avg_5", "id")
w10 = Window.partitionBy("product_id").orderBy(F.col("revenue_avg_5").desc(), "id")
w11 = Window.orderBy("revenue")

base = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 4]))
    .select("*", F.coalesce("units", F.lit(0)).alias("units_filled"))
)

metrics = (
    base
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "units_filled",
        F.last("revenue", ignorenulls=True).over(w_running).alias("last_known_revenue"),
        F.avg("units").over(w2).alias("units_avg_6"),
        F.cume_dist().over(w3).alias("units_cume_dist"),
        F.avg("revenue").over(w4).alias("revenue_avg_3_prior"),
        F.percent_rank().over(w5).alias("units_filled_percent_rank"),
    )
)

scored = (
    metrics
    .filter(F.col("store_id").isin([1, 3, 4, 5]))
    .select(
        "id",
        "product_id",
        "revenue",
        "units_filled",
        "units_avg_6",
        "units_cume_dist",
        "revenue_avg_3_prior",
        "units_filled_percent_rank",
        F.row_number().over(w6).alias("event_seq"),
        F.lead("revenue_avg_3_prior", 1).over(w7).alias("next_revenue_avg_3_prior"),
        F.avg("revenue").over(w8).alias("revenue_avg_5"),
    )
)

joined = (
    scored
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.category"))
)

enriched = (
    joined
    .select(
        "id",
        "product_id",
        "revenue",
        "units_filled",
        "units_avg_6",
        "units_filled_percent_rank",
        "revenue_avg_5",
        F.lag("revenue", 1).over(w9).alias("prev_revenue"),
        F.row_number().over(w10).alias("event_seq_2"),
        F.cume_dist().over(w11).alias("revenue_cume_dist"),
    )
)

enriched_store_sales = (
    enriched
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.product_id",
        "e.prev_revenue",
        F.col("d.category"),
        F.col("d.price"),
        F.col("d.product_name"),
    )
)

result = (
    enriched_store_sales
    .groupBy("product_id")
    .agg(
        F.round(F.avg("prev_revenue"), 2).alias("avg_prev_revenue"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(F.col("product_id").asc_nulls_first())
)
