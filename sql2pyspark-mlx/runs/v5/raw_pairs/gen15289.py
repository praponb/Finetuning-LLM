from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "status")
    .intersectAll(
        df_orders
        .filter(F.col("amount") > 480)
        .select("id", "status")
    )
)
