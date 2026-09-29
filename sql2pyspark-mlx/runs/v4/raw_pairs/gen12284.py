from pyspark.sql import functions as F

kept_keys = (
    df_inventory
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("last_restock") > "2023-09-01")
        .select("product_id", "id")
    )
)

reconciled = (
    df_inventory
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("activity_month", F.date_trunc("month", "last_restock"))
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 250, "PREMIUM").otherwise("STANDARD"))
    .withColumn("year_start", F.date_trunc("year", "last_restock"))
    .groupingSets(
        [["activity_month", "quantity_tier", "year_start"], ["activity_month", "quantity_tier"], ["activity_month"], []],
        "activity_month",
        "quantity_tier",
        "year_start",
    )
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
    )
    .filter(F.col("avg_quantity") > 0)
)
