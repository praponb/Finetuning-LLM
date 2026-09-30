from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([7, 28, 31]) & (F.col("category") != "Books") & (F.col("stock_qty") == 143))
    .agg(
        F.sum(F.when(F.col("brand") == "Umbrella", F.col("price")).otherwise(0)).alias("umbrella_price"),
    )
    .orderBy(F.col("umbrella_price").desc())
)
