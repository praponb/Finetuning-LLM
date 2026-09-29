from pyspark.sql import functions as F

remaining_keys = (
    df_store_sales
    .select("product_id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue").between(2600, 2900))
        .select("product_id")
    )
)

verified = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy(F.date_trunc("QUARTER", "sale_date").alias("quarter_start"))
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy(F.col("distinct_ids_2").desc())
)
