from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 39)
    .select("id", "status", F.coalesce("cost", F.lit(0)).alias("cost_filled"))
)
