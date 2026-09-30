from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.filter(~F.col("segment").isin(["SMB", "Consumer"])).alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left_semi")
    .filter(~F.col("sessions.country").isin(["DE", "TH", "US"]))
    .select(F.col("sessions.id"), F.col("sessions.device"))
    .orderBy(F.col("device").desc())
)
