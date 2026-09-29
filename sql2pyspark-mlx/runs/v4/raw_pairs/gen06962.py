from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("store_id") == 2)

summary = (
    base
    .groupBy(
        "store_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(F.avg("units").alias("avg_units"))
)

result = summary
