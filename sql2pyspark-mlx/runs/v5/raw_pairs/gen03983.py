from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("cost") >= 550, "LARGE").otherwise("SMALL").alias("cost_tier"),
    )
    .agg(
        F.sum("stock_qty").alias("total_stock_qty"),
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
    )
    .orderBy("stock_qty_bit_or")
)
