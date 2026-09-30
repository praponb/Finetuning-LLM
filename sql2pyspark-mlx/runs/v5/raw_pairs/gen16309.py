from pyspark.sql import functions as F

result = (
    df_orders
    .select("channel", "status")
    .intersect(
        df_orders
        .filter(F.col("amount") >= 730)
        .select("channel", "status")
    )
)
