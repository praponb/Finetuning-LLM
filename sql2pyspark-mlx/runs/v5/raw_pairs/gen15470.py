from pyspark.sql import functions as F

result = (
    df_orders
    .select("id")
    .intersectAll(
        df_orders
        .filter(F.col("amount") > 1300)
        .select("id")
    )
)
