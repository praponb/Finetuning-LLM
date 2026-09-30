from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("segment").isNotNull() & F.col("segment").isin(["Corporate", "Consumer"]))
    .withColumn("country_name_key", F.concat_ws(",", "country", "name"))
    .withColumn("is_active_label", F.when(F.col("is_active"), "Yes").otherwise("No"))
    .withColumn("city_filled", F.coalesce("city", F.lit("UNKNOWN")))
)

result = (
    base
    .groupBy("city", "country")
    .pivot("segment", ["Corporate", "SMB"])
    .agg(F.sum("age"))
)
