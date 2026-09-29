from pyspark.sql import functions as F

result = (
    df_orders
    .select("id")
    .exceptAll(
        df_orders
        .filter(F.col("status") == "RETURNED")
        .select("id")
    )
)
