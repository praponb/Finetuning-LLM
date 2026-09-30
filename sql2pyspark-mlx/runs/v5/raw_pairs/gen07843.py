from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("country", "segment").orderBy(F.col("age").desc_nulls_last())
w2 = Window.partitionBy("country").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.to_date(F.lit("2023-09-13"))) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

result = (
    filtered
    .select(
        "id",
        "segment",
        "is_active",
        "city_clean",
        F.dense_rank().over(w_part).alias("age_dense_rank"),
        F.last("age", ignorenulls=True).over(w2).alias("last_known_age"),
    )
)
