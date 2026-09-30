from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.filter(F.col("segment").isNull()).alias("c"), F.col("s.customer_id") == F.col("c.id"), "left_semi")
    .filter(~F.col("s.browser").isin(["Chrome", "Firefox", "Edge"]))
    .select(F.col("s.id"), F.col("s.customer_id"))
    .orderBy(F.col("customer_id").desc())
)
