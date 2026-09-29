from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 590)
    .select("status")
    .subtract(df_shipments.select("status"))
    .orderBy("status")
)
