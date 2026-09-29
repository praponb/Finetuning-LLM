from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "status",
        "amount",
        "discount",
        F.initcap(F.trim("status")).alias("status_title"),
    )
    .orderBy(F.col("amount").desc(), "id")
    .limit(3)
)
