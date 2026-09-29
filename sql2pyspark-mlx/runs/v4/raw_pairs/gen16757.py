from pyspark.sql import functions as F

remaining_keys = (
    df_products
    .select("supplier_id", "id")
    .subtract(
        df_products
        .filter(F.col("cost").between(270, 330))
        .select("supplier_id", "id")
    )
)

verified = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupBy("supplier_id")
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.max(F.when(F.col("category") == "Electronics", F.col("stock_qty"))).alias("electronics_max_stock_qty"),
    )
    .orderBy(F.col("electronics_max_stock_qty").desc_nulls_last())
)
