from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("store_id").orderBy("quantity_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("product_id").orderBy("last_restock_period", "id")
w5 = Window.partitionBy("product_id").orderBy("last_restock_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_inventory
    .filter(F.col("product_id").isin([8, 10]))
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 0.85), 2))
)

with_windows = (
    base
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w))
    .withColumn("running_quantity_scaled", F.sum("quantity_scaled").over(w2))
    .withColumn("first_known_quantity_scaled", F.first("quantity_scaled", ignorenulls=True).over(w3))
)

base_inventory = (
    with_windows
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-03-22")))
    .select(
        "id",
        "product_id",
        "quantity",
        "quantity_scaled",
        "prev_quantity",
        "running_quantity_scaled",
        "first_known_quantity_scaled",
        F.date_trunc("year", "last_restock").alias("last_restock_period"),
    )
)

prev_events = (
    base_inventory
    .withColumn("prev_last_restock_period", F.lag("last_restock_period").over(w4))
)

flagged = (
    prev_events
    .withColumn("is_new_session", F.when(F.col("prev_last_restock_period").isNull() | (F.col("last_restock_period") > F.col("prev_last_restock_period") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w5))

summary = (
    sessionized
    .groupBy(
        F.date_trunc("YEAR", "last_restock_period").alias("year_start"),
        F.when(F.col("prev_quantity") >= 239, "HIGH").otherwise("LOW").alias("prev_quantity_tier"),
    )
    .agg(
        F.percentile_approx("running_quantity_scaled", [0.25, 0.5, 0.75]).alias("running_quantity_scaled_quartiles"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
    )
)

result = (
    summary
    .select("year_start", "prev_quantity_tier", "quantity_p95")
    .orderBy(F.col("quantity_p95").desc(), "prev_quantity_tier")
)
