from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "Kerry") | (F.col("cost") > 58))
    .orderBy(F.col("order_id").desc(), "id")
    .limit(3)
)
