from pyspark.sql import functions as F

result = (
    df_inventory.alias("i")
    .join(df_store_sales.alias("ss"), (F.col("i.store_id") == F.col("ss.store_id")) & (F.col("i.product_id") == F.col("ss.product_id")), "left")
    .groupBy(
        F.col("ss.product_id"),
        F.quarter(F.col("ss.sale_date")).alias("sale_quarter"),
    )
    .agg(F.sum(F.col("i.quantity")).alias("total_quantity"))
    .filter(F.col("total_quantity") > 410)
    .orderBy(F.col("sale_quarter").desc())
)
