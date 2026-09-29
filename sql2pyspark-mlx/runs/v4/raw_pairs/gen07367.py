from pyspark.sql import functions as F

filtered_customers = df_customers.filter(~F.col("country").isin(["UK", "JP"]))

agg = (
    filtered_customers
    .groupBy("city", "country")
    .agg(F.round(F.avg("credit_limit"), 2).alias("avg_credit_limit"))
)

result = (
    agg
    .filter(F.col("avg_credit_limit") > 41)
    .select("city", "country", "avg_credit_limit")
    .orderBy("country", "city")
    .limit(20)
)
