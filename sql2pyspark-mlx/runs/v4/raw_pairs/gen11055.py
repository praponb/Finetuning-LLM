from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("brand", "supplier_id")
    .agg(
        F.min("launch_date").alias("first_launch_date"),
        F.round(((100 * F.sum(F.when(F.col("brand") == "Globex", 1).otherwise(0))) / F.count("*")), 1).alias("pct_globex"),
        F.sum(F.when(F.col("brand") == "Stark", F.col("stock_qty")).otherwise(0)).alias("stark_stock_qty"),
    )
    .filter(F.col("stark_stock_qty") < 21)
    .orderBy(F.col("supplier_id").desc())
)
