from pyspark.sql import functions as F

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-03-05").cast("date")) & (F.col("last_restock") >= "2023-10-14"))
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 0.9), 2))
)

result = (
    base
    .select("id")
    .intersectAll(
        df_inventory
        .filter(F.col("store_id").isin([1, 5]))
        .select("id")
    )
)
