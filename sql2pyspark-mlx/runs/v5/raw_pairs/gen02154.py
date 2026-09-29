from pyspark.sql import functions as F

result = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id").eqNullSafe(F.col("d.id")), "inner")
    .select("a.*", F.col("d.segment"))
)
