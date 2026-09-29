from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("revenue_per_unit").desc(), "id")
w_grp = Window.partitionBy("store_id", "product_id").orderBy("sale_date", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("revenue")
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.min("quantity").alias("min_inventory_quantity"))

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-07-25"))) & F.col("product_id").isin([3, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.avg("revenue").over(w).alias("product_id_avg_revenue"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

metrics = (
    scored
    .withColumn("units_avg_2", F.avg("units").over(w_grp))
    .withColumn("revenue_cume_dist", F.cume_dist().over(w4))
)

grouped = (
    metrics
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 3400, "LARGE").otherwise("SMALL"))
    .cube("revenue_tier", "store_id")
    .agg(
        F.percentile_approx("product_id_avg_revenue", [0.25, 0.5, 0.75]).alias("product_id_avg_revenue_quartiles"),
        F.count("*").alias("event_count"),
        F.min("units_avg_2").alias("min_units_avg_2"),
        F.max("event_seq").alias("max_event_seq"),
        F.grouping("revenue_tier").alias("grouping_id"),
    )
)

result = (
    grouped
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "revenue_tier",
        "store_id",
        "event_count",
        "min_units_avg_2",
        "max_event_seq",
        "grouping_id",
        F.col("min_inventory_quantity"),
    )
    .orderBy(F.col("grouping_id").desc(), "min_units_avg_2")
)
