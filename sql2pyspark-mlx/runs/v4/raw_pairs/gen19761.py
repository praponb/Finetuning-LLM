from pyspark.sql import functions as F

result = (
    df_orders
    .select("id", "status")
    .exceptAll(
        df_orders
        .filter(F.col("discount") >= 0.33)
        .select("id", "status")
    )
)
