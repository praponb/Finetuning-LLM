from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("product_id")
    .subtract(
        df_inventory
        .filter(F.col("quantity") > 141)
        .select("product_id")
    )
)

verified = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube(
        F.date_trunc("year", "last_restock").alias("year_start"),
        "product_id",
        "store_id",
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
    )
    .filter(F.col("distinct_product_ids") > 1)
    .orderBy("product_id")
)
