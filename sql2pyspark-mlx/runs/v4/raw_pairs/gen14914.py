from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "country").orderBy("segment_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("segment_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("age", "id")
w_hist = Window.partitionBy("segment", "city").orderBy("segment_length", "id").rowsBetween(-6, -1)
w5 = Window.orderBy("credit_limit", "id")

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-05-08").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "is_active",
        "credit_limit",
        "age",
        F.length("segment").alias("segment_length"),
    )
)

result = (
    filtered
    .select(
        "*",
        F.last("city", ignorenulls=True).over(w).alias("last_known_city"),
        F.first("city", ignorenulls=True).over(w2).alias("first_known_city"),
        F.lag("age", 1).over(w3).alias("prev_age"),
        F.avg("age").over(w_hist).alias("age_avg_6_prior"),
        F.row_number().over(w5).alias("row_seq"),
    )
)
