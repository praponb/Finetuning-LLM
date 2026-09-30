from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("balance").between(27000, 71000))
    .select(F.col("opened_date").alias("event_date"), F.lit("account").alias("source"))
    .unionByName(
        df_shipments
        .select(
        F.col("shipped_date").alias("event_date"),
        F.lit("shipment").alias("source"),
    )
    )
    .orderBy(F.col("event_date").desc())
)
