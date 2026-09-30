from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["DHL", "Flash", "Kerry"]))
    .select("cost")
    .unionByName(df_products.select(F.col("price").alias("cost")))
)
