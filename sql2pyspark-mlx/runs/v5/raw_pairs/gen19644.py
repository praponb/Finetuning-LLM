from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "inner")
    .groupBy(F.year(F.col("ss.sale_date")).alias("sale_year"))
    .agg(
        (F.max(F.col("i.quantity")) - F.min(F.col("i.quantity"))).alias("quantity_range"),
        F.max(F.col("i.last_restock")).alias("latest_last_restock"),
    )
    .filter(F.col("quantity_range") >= 64)
    .orderBy("latest_last_restock", "sale_year")
    .limit(20)
)
