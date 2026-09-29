from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy(
        "product_id",
        F.when(F.col("unit_price") >= 420, "LARGE").otherwise("SMALL").alias("unit_price_tier"),
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.corr("unit_price", "quantity").alias("corr_unit_price_quantity"),
        F.count("*").alias("row_count"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("num_rows") > 1)
    .orderBy(F.col("unit_price_tier").asc_nulls_last())
)
