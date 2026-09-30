from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select("status")
    .unionByName(df_shipments.filter(F.col("cost").between(27, 93)).select("status"))
)
