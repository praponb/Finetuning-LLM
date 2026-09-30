from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.16)
    .select("status")
    .intersect(df_orders.filter(F.col("status") == "COMPLETED").select("status"))
)
