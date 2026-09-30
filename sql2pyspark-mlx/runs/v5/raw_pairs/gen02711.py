from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("segment")

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
        F.last("city", ignorenulls=True).over(w).alias("last_known_city"),
        F.count("age").over(w2).alias("segment_count_age"),
    )
)

result = (
    metrics
    .select(
        "id",
        "name",
        "email",
        "country",
        "signup_date",
        "is_active",
        "age",
        "last_known_city",
    )
)
