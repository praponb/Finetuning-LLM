from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy()

result = (
    df_customers
    .filter(F.length("email") >= 12)
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.sum("age").alias("total_age"),
        F.count("*").alias("cnt"),
    )
    .withColumn("pct_of_total", F.round(((F.col("total_age") * 100) / F.sum("total_age").over(w)), 2))
    .orderBy("cnt", F.col("pct_of_total").desc(), "signup_year")
    .limit(50)
)
