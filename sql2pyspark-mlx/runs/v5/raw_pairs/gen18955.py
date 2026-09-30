from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter((~F.col("brand").eqNullSafe(F.lit("Umbrella"))) & F.col("price").isNull())
    .withColumn("stock_qty_plus_price", (F.col("stock_qty") + F.col("price")))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("N/A")))
)

result = (
    cleaned
    .select("id", "brand", "cost", "price", "stock_qty_plus_price")
    .unpivot(
        ids=["id", "brand"],
        values=["cost", "price", "stock_qty_plus_price"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
)
