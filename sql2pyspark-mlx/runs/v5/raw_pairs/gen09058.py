from pyspark.sql import functions as F

remaining_keys = (
    df_store_sales
    .select("id")
    .intersectAll(
        df_store_sales
        .filter(F.col("sale_date") < "2024-11-01")
        .select("id")
    )
)

reconciled = (
    df_store_sales
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select("store_id", "sale_date", "units", "revenue")
    .orderBy(F.col("revenue").desc())
)
