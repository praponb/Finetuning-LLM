from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("city")
w3 = Window.partitionBy("country")

enriched = (
    df_customers
    .filter(F.col("city") == "Chiang Mai")
    .withColumn("age_avg_2_prior", F.avg("age").over(w))
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w2))
    .withColumn("country_max_age", F.max("age").over(w3))
)

result = (
    enriched
    .select(
        "id",
        "name",
        "email",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        "country_max_age",
        F.when(F.col("segment").rlike("^(Corporate|Consumer)$"), "EXTENDED").when(F.col("segment").rlike("^(SMB)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("segment_zone"),
    )
    .orderBy(F.col("credit_limit").desc_nulls_last(), "segment")
)
