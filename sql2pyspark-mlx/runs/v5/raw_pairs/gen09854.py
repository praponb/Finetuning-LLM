from pyspark.sql import functions as F

result = (
    df_store_sales
    .alias("a")
    .join(df_stores.alias("d"), F.col("a.store_id") == F.col("d.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.product_id",
        "a.sale_date",
        "a.units",
        F.col("d.store_name"),
    )
)
