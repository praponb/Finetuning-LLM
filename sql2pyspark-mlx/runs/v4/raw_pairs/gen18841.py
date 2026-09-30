from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("Gra%") & (F.col("age") < 36) & F.col("is_active"))
    .select(
        "id",
        "country",
        "email",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
    )
)
