from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~((F.year("sale_date") == 2024) & ((F.col("revenue") < 3100) | F.col("store_id").isin([1, 2, 4]))))
    .groupBy(
        F.date_format("sale_date", "yyyy-MM").alias("sale_month"),
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.max("sale_date").alias("latest_sale_date"),
    )
)
