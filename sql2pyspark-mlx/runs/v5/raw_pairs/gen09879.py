from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment", "city").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("segment")

metrics = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "age",
        F.first("city", ignorenulls=True).over(w).alias("first_known_city"),
        F.sum("age").over(w2).alias("cumulative_age"),
        F.corr("age", "credit_limit").over(w_grp).alias("corr_age_credit_limit"),
    )
)

result = (
    metrics
    .select(
        "id",
        "name",
        "email",
        "country",
        "is_active",
        "age",
        "first_known_city",
        "cumulative_age",
        F.when(F.col("country").rlike("^(SG|US)$"), "CORE").otherwise("GROUP_B").alias("country_zone"),
    )
    .orderBy(F.col("country_zone").desc_nulls_last())
)
