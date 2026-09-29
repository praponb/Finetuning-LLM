from pyspark.sql import functions as F

result = (
    df_products
    .select("id", "brand", F.col("price").alias("PRICE_VAL"), "cost")
    .unpivot(
        ids=["id", "brand"],
        values=["PRICE_VAL", "cost"],
        variableColumnName="metric",
        valueColumnName="value",
    )
)
