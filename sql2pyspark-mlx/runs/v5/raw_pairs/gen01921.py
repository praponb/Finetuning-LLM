from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") == "RETURNED") & (F.col("amount") < 1320))
    .select("discount")
)
