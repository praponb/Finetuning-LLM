from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("s")
    .join(df_stores.alias("p2"), F.col("s.store_id") == F.col("p2.id"), "inner")
    .filter(F.col("p2.store_name").like("M%"))
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.units",
        "s.revenue",
        F.col("p2.opened_date"),
        F.col("p2.city"),
    )
)
