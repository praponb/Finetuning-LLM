from pyspark.sql import functions as F

remaining_keys = (
    df_store_sales
    .select("id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([8, 10]))
        .select("id", "store_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = verified.select("id", "store_id", "sale_date", "revenue")
