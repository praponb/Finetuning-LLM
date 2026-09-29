from pyspark.sql import functions as F

result = (
    df_store_sales.alias("ss")
    .join(df_products.alias("p"), F.col("p.id") == F.col("ss.product_id"), "right")
    .groupBy(
        F.when(F.year(F.col("p.launch_date")) < 2024, "before_2024").when(F.year(F.col("p.launch_date")) == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        F.col("p.brand"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("ss.sale_date")).alias("last_sale_date"),
    )
    .filter(F.col("cnt") > 4)
)
