from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").desc_nulls_first())
w2 = Window.partitionBy("city").orderBy("credit_limit", "id")
w_ordered = Window.partitionBy("country").orderBy("signup_date", "id")
w4 = Window.orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment").orderBy(F.col("age").desc(), "id")

scored = (
    df_customers
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w))
    .withColumn("credit_limit_bucket", F.ntile(4).over(w2))
    .withColumn("next_age", F.lead("age", 2, 0).over(w_ordered))
    .withColumn("first_known_segment", F.first("segment", ignorenulls=True).over(w4))
    .withColumn("age_bucket", F.ntile(5).over(w5))
)

result = (
    scored
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
        F.when(F.col("city").rlike("^(Berlin|Singapore)$"), "GROUP_B").otherwise("EXTENDED").alias("city_zone"),
    )
    .orderBy("signup_date")
)
