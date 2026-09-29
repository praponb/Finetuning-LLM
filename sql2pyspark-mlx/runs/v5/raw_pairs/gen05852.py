from pyspark.sql import functions as F

cleaned = (
    df_inventory
    .filter(F.col("store_id").isin([3, 4]))
    .select("*", F.ceil("quantity").alias("quantity_whole"))
)

base = (
    cleaned
    .filter(F.col("last_restock") > F.lit("2023-04-07").cast("date"))
    .withColumn("quantity_whole_minus_quantity", (F.col("quantity_whole") - F.col("quantity")))
)

base_inventory = (
    base
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-07-04")))
    .select(
        "*",
        F.greatest("quantity_whole_minus_quantity", "quantity").alias("greatest_quantity_whole_minus_quantity_quantity"),
        (F.col("quantity_whole") * F.col("quantity")).alias("quantity_whole_times_quantity"),
    )
)

enriched = (
    base_inventory
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select("a.*", F.col("d.price"), F.col("d.brand"))
)

enriched_inventory = (
    enriched
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.launch_date") < "2025-09-01")
    .select(
        "e.id",
        "e.product_id",
        "e.last_restock",
        "e.greatest_quantity_whole_minus_quantity_quantity",
        "e.quantity_whole_times_quantity",
        F.col("p.launch_date"),
        F.col("p.cost"),
    )
)

result = (
    enriched_inventory
    .groupBy(
        F.when(F.col("cost") >= 550, "HIGH").otherwise("LOW").alias("cost_tier"),
        F.date_trunc("year", "launch_date").alias("year_start"),
        F.date_trunc("month", "last_restock").alias("activity_month"),
    )
    .agg(
        F.avg("cost").alias("avg_cost"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("year_start").asc_nulls_last())
)
