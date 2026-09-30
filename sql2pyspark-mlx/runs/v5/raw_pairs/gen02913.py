from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2023-03-20").cast("date")) & F.col("store_id").isin([1, 2, 3, 6]))
    .withColumn("units_whole", F.floor("units"))
    .withColumn("sale_date_label", F.date_format("sale_date", "yyyy-MM"))
)

result = (
    cleaned
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.store_id",
        "e.units",
        "e.revenue",
        F.col("p.city"),
        F.col("p.opened_date"),
    )
)
