from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(-3, -1)

enriched = (
    df_customers
    .select(
        "id",
        "name",
        "country",
        "segment",
        "is_active",
        F.sum("age").over(w).alias("cumulative_age"),
        F.first("segment", ignorenulls=True).over(w2).alias("first_known_segment"),
        F.avg("credit_limit").over(w3).alias("credit_limit_avg_3_prior"),
    )
)

result = (
    enriched
    .select(
        "id",
        "country",
        "segment",
        "is_active",
        "cumulative_age",
        "first_known_segment",
        F.when(F.col("country").rlike("^(TH|JP)$"), "CORE").when(F.col("country").rlike("^(US|SG)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("country_zone"),
    )
)
