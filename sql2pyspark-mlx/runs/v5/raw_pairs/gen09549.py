from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("sqft", "id")
w2 = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id").orderBy("quantity_filled_int", "id").rowsBetween(-5, Window.currentRow)
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.sum("revenue").alias("sum_store_sale_revenue"))

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-09-11").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

cleaned = (
    valid
    .filter(F.col("quantity_filled").between(20, 60))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "quantity_filled",
        F.col("quantity_filled").cast("int").alias("quantity_filled_int"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.quantity",
        "s.quantity_filled",
        "s.quantity_filled_int",
        F.col("p.store_name"),
        F.col("p.sqft"),
    )
)

with_windows = (
    enriched
    .filter(F.col("store_id").isin([1, 2, 5, 6]))
    .select(
        "id",
        "store_id",
        "quantity_filled",
        "sqft",
        F.row_number().over(w).alias("row_seq"),
        F.min("quantity_filled_int").over(w2).alias("store_id_min_quantity_filled_int"),
        F.avg("quantity_filled_int").over(w3).alias("quantity_filled_int_avg_5"),
    )
)

summary = (
    with_windows
    .groupBy(
        F.when(F.col("store_id_min_quantity_filled_int") >= 71, "PREMIUM").otherwise("STANDARD").alias("store_id_min_quantity_filled_int_tier"),
        "store_id",
    )
    .agg(
        F.percentile_approx("quantity_filled", [0.25, 0.5, 0.75]).alias("quantity_filled_quartiles"),
        F.round(F.avg("store_id_min_quantity_filled_int"), 2).alias("avg_store_id_min_quantity_filled_int"),
        F.round(F.avg("quantity_filled"), 2).alias("avg_quantity_filled"),
        F.max("quantity_filled").alias("max_quantity_filled"),
        F.percentile_approx("quantity_filled_int_avg_5", [0.25, 0.5, 0.75]).alias("quantity_filled_int_avg_5_quartiles"),
    )
)

result = (
    summary
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "store_id_min_quantity_filled_int_tier",
        "store_id",
        "quantity_filled_quartiles",
        "max_quantity_filled",
        F.col("sum_store_sale_revenue"),
    )
    .orderBy("max_quantity_filled")
)
