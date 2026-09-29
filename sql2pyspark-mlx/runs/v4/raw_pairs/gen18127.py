from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("status").isin(["PENDING", "SHIPPED"]))
    .select("id", "customer_id", "discount")
    .orderBy("customer_id", F.col("discount").desc(), "id")
    .limit(20)
)
