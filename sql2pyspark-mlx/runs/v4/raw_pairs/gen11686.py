from pyspark.sql import functions as F

valid_customers = df_customers.filter(F.col("credit_limit") < 10600)

agg = (
    valid_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
)

result = (
    agg
    .select("signup_month", "city_list")
    .orderBy("signup_month")
    .limit(20)
)
