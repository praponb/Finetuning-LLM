from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2024-03-02"))) & F.col("product_id").isin([6, 10]))
    .select("*", (F.col("units") * F.col("revenue")).alias("revenue_per_unit"))
)

result = (
    cleaned
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.city"))
)
