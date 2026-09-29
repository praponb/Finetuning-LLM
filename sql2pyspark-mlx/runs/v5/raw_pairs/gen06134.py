from pyspark.sql import functions as F

base = df_store_sales.filter(F.col("product_id").isin([8, 10]))

agg = (
    base
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
)

result = agg.filter(F.col("units_range") >= 64)
