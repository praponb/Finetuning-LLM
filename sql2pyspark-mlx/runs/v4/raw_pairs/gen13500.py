from pyspark.sql import functions as F

remaining_keys = (
    df_inventory
    .select("product_id")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([1, 3, 5, 10]))
        .select("product_id")
    )
)

kept_rows = (
    df_inventory
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .groupBy("product_id")
    .agg(
        F.count("*").alias("cnt"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
    .orderBy(F.col("product_id").desc_nulls_last())
)
