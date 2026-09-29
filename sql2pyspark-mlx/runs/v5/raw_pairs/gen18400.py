from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sale_date", "id")
w2 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("next_revenue").desc())
w4 = Window.partitionBy("region", "city").orderBy("prev_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("region").orderBy("row_num", "id")
w6 = Window.partitionBy("city").orderBy("row_num", "id").rowsBetween(-2, -1)
w7 = Window.partitionBy("store_id").orderBy("revenue", "id")
w8 = Window.orderBy(F.col("prev_revenue").desc())

with_windows = (
    df_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        F.lead("units").over(w).alias("next_units"),
        F.lead("revenue").over(w).alias("next_revenue"),
        F.lag("revenue").over(w).alias("prev_revenue"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("units").over(w2).alias("running_units"),
    )
)

enriched = (
    with_windows
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.opened_date").between("2025-01-14", "2025-08-01"))
    .select("e.*", F.col("p.region"), F.col("p.store_name"), F.col("p.sqft"))
)

joined = (
    enriched
    .alias("s")
    .join(df_stores.alias("b"), F.col("s.store_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.revenue",
        "s.next_revenue",
        "s.prev_revenue",
        "s.event_seq",
        "s.region",
        F.col("b.city"),
    )
)

enriched_store_sales = (
    joined
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.store_name"))
)

latest = (
    enriched_store_sales
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

enriched_store_sales_2 = (
    latest
    .withColumn("first_known_event_seq", F.first("event_seq", ignorenulls=True).over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
    .withColumn("prev_revenue_avg_2_prior", F.avg("prev_revenue").over(w6))
)

metrics = (
    enriched_store_sales_2
    .select(
        "*",
        F.ntile(4).over(w7).alias("revenue_bucket"),
        F.dense_rank().over(w8).alias("prev_revenue_dense_rank"),
    )
)

grouped = (
    metrics
    .groupBy(
        F.when(F.col("seq_no") >= 7, "HIGH").otherwise("LOW").alias("seq_no_tier"),
        F.when(F.col("revenue") >= 3900, "LARGE").otherwise("SMALL").alias("revenue_tier"),
    )
    .agg(
        F.count(F.when(F.col("region") == "North", 1)).alias("north_count"),
        F.sum(F.when(F.col("city") == "New York", F.col("revenue_bucket")).otherwise(0)).alias("new_york_revenue_bucket"),
        F.avg(F.when(F.col("region") == "East", F.col("revenue"))).alias("east_avg_revenue"),
        F.count("*").alias("num_rows"),
        F.count("*").alias("num_rows_2"),
    )
)

result = (
    grouped
    .select(
        "seq_no_tier",
        "revenue_tier",
        "north_count",
        "new_york_revenue_bucket",
        "num_rows",
        "num_rows_2",
    )
    .orderBy(F.col("seq_no_tier").desc())
)
