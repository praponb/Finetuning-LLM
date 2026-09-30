from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select("method", F.initcap(F.trim("status")).alias("status_title"))
    .orderBy(F.col("status_title").desc())
)
