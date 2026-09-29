from pyspark.sql import functions as F

src = df_customers.filter(F.col("age") < 55)

summary = (
    src
    .groupBy("country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "UK", 1).otherwise(0))) / F.count("*")), 1).alias("pct_uk"),
    )
)

result = summary.select("country", "pct_uk")
