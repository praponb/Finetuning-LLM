from pyspark.sql import functions as F

flattened = df_price_tiers.select("id", "currency", F.explode("discounts").alias("val"))

result = (
    flattened
    .groupBy(F.when(F.col("val") >= 74, "LARGE").otherwise("SMALL").alias("val_tier"))
    .agg(
        F.sum("val").alias("total_val"),
        F.max(F.when(F.col("currency") == "EUR", F.col("val"))).alias("eur_max_val"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("total_val") > 0)
)
