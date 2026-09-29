from pyspark.sql import functions as F

result = (
    df_orders
    .select("status")
    .intersect(
        df_accounts
        .filter(F.col("balance").between(23000, 75000))
        .select("status")
    )
    .orderBy(F.col("status").desc())
)
