from pyspark.sql import functions as F

kept_keys = (
    df_store_sales
    .select("store_id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue").between(1300, 2500))
        .select("store_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.store_id") == F.col("k.store_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select("store_id", "product_id", "sale_date")
    .orderBy("product_id", F.col("store_id").desc())
)
