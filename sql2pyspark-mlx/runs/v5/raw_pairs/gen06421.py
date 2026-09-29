from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country", "segment").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("country")

scored = (
    df_customers
    .withColumn("last4_avg_credit_limit", F.avg("credit_limit").over(w_grp))
    .withColumn("last4_min_credit_limit", F.min("credit_limit").over(w_grp))
    .withColumn("last4_count_credit_limit", F.count("credit_limit").over(w_grp))
    .withColumn("corr_age_credit_limit", F.corr("age", "credit_limit").over(w2))
)

result = (
    scored
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "age",
        "last4_avg_credit_limit",
        "last4_min_credit_limit",
    )
    .orderBy(F.col("age").desc_nulls_last(), F.col("last4_avg_credit_limit").desc())
)
