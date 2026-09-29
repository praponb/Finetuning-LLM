from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "country").orderBy("age_filled", "id")
w2 = Window.orderBy(F.col("credit_limit").desc(), "id")
w3 = Window.partitionBy("city").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "segment").orderBy("age_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("segment")

filtered = (
    df_customers
    .filter(F.col("city").isNotNull() & ~F.col("is_active"))
    .withColumn("age_filled", F.coalesce("age", F.lit(0)))
    .withColumn("resolved_city", F.coalesce("city", F.lit("OTHER")))
)

result = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("event_seq"),
        F.row_number().over(w2).alias("row_seq"),
        F.last("credit_limit", ignorenulls=True).over(w3).alias("last_known_credit_limit"),
        F.first("city", ignorenulls=True).over(w4).alias("first_known_city"),
        F.round((F.col("age") / F.sum("age").over(w5)), 4).alias("age_share"),
    )
)
