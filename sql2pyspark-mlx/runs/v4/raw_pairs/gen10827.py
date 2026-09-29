from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("signup_date", "id")
w3 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter(F.col("signup_date") >= F.to_date(F.lit("2024-03-03")))
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        F.upper("city").alias("city_upper"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("resolved_segment"),
    )
)

result = (
    filtered
    .withColumn("last_known_segment", F.last("segment", ignorenulls=True).over(w))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("first_known_segment", F.first("segment", ignorenulls=True).over(w3))
)
