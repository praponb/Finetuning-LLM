from pyspark.sql import functions as F

result = (
    df_orders
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id").eqNullSafe(F.col("b.id")), "inner")
    .filter(F.col("b.opened_date").between("2024-02-14", "2024-10-11"))
    .select(
        "src.id",
        "src.customer_id",
        "src.store_id",
        "src.order_date",
        "src.amount",
        "src.discount",
        F.col("b.sqft"),
        F.col("b.city"),
    )
)
