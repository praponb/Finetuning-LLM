from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.col("brand").isin(["Umbrella", "Acme"]), "core").otherwise("other").alias("brand_group"),
    )
    .agg(
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.sum(F.when(F.col("category") == "Fashion", F.col("stock_qty")).otherwise(0)).alias("fashion_stock_qty"),
    )
    .filter(F.col("fashion_stock_qty") < 25)
    .orderBy("brand_group")
    .limit(50)
)
