from pyspark.sql import functions as F

base = df_customers.filter(F.col("email").like("%@GMAIL.COM") | (F.col("country") == "DE"))

summary = (
    base
    .groupBy(
        F.when(F.col("segment").isin(["Consumer", "SMB"]), "group_a").otherwise("other").alias("segment_group"),
        "segment",
    )
    .agg(F.avg("age").alias("avg_age"))
)

result = (
    summary
    .select("segment_group", "segment", "avg_age")
    .orderBy("segment_group", "segment")
    .limit(5)
)
