from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.discount").desc(), F.col("c.id").desc())

base = (
    df_customers
    .filter(F.col("credit_limit") >= 15200)
    .withColumn("email_filled", F.coalesce("email", F.lit("Unknown")))
    .withColumn("resolved_city", F.coalesce("city", F.lit("UNKNOWN")))
)

result = (
    base
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.country", "s.age", F.col("c.status"))
)
