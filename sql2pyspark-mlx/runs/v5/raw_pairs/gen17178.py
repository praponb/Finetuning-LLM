from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("store_id").isin([2, 4, 5]))

summary = (
    base
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        "product_id",
    )
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.sum("units").alias("total_units"),
    )
)

result = (
    summary
    .filter(F.col("total_units") < 70)
    .select("sale_cohort", "product_id", "last_sale_date", "total_units")
)
