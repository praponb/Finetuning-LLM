from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city", "country")
    .agg((F.max("age") - F.min("age")).alias("age_range"))
    .filter(F.col("age_range") > 21)
    .orderBy(F.col("age_range").desc(), "city", "country")
    .limit(10)
)
