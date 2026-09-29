from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("segment")
w3 = Window.partitionBy("city", "country")
w_ordered = Window.partitionBy("country").orderBy("age", "id")

cleaned = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-09-09"))) & (F.col("city") == "Chiang Mai"))
    .withColumn("least_age_credit_limit", F.least("age", "credit_limit"))
    .withColumn("country_title", F.initcap(F.trim("country")))
)

result = (
    cleaned
    .withColumn("age_share", F.round((F.col("age") / F.sum("age").over(w)), 4))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w2))
    .withColumn("credit_limit_share", F.round((F.col("credit_limit") / F.sum("credit_limit").over(w3)), 4))
    .withColumn("next_age", F.lead("age", 1).over(w_ordered))
)
