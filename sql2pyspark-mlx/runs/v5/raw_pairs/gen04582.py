from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_customers
    .filter(F.col("name").like("%Srisuk%"))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_credit_limit") * 100) / F.sum("total_credit_limit").over(w)), 2))
    .orderBy("signup_month")
    .limit(10)
)
