from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id")
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_customers
    .select(
        "*",
        F.ntile(5).over(w).alias("age_bucket"),
        F.first("credit_limit", ignorenulls=True).over(w2).alias("first_known_credit_limit"),
    )
)

result = (
    metrics
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
        F.when(F.col("segment").rlike("^(SMB|Consumer)$"), "GROUP_B").when(F.col("segment").rlike("^(Corporate)$"), "CORE").otherwise("PRIORITY").alias("segment_zone"),
    )
)
