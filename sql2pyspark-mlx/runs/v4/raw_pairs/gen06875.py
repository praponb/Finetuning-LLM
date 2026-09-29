from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 2, 9, 10]))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        F.quarter("sale_date").alias("sale_quarter"),
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("sale_quarter").desc())
)
