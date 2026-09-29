from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy(F.col("age").asc_nulls_last(), "id")

deduped = (
    df_customers
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .groupBy(
        "city",
        F.when(F.col("credit_limit") >= 16300, "HIGH").otherwise("LOW").alias("credit_limit_tier"),
    )
    .agg(
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("country") == "DE", 1)), F.lit(0)), 2).alias("credit_limit_per_de"),
        F.count(F.when(F.col("segment") == "Consumer", 1)).alias("consumer_count"),
        F.bit_and("age").alias("age_bit_and"),
        F.percentile_approx("age", 0.9).alias("age_p90"),
    )
    .orderBy(F.col("consumer_count").asc_nulls_last())
)
