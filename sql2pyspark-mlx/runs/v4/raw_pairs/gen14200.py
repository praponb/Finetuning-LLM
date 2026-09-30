from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date", F.lit("shipment").alias("source"))
    .union(
        df_accounts
        .filter(F.col("opened_date").between("2023-06-01", "2023-08-27"))
        .select("opened_date", F.lit("account").alias("source"))
    )
    .orderBy("shipped_date")
)
