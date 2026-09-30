from pyspark.sql import functions as F

joined = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.supplier_name"), F.col("p.rating"), F.col("p.city"))
)

result = (
    joined
    .groupBy("city")
    .agg(
        F.countDistinct("brand").alias("distinct_brands"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("rating")).otherwise(0)).alias("tokyo_rating"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
    )
    .filter(F.col("tokyo_rating") >= 0)
)
