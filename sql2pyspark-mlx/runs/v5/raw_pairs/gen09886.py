from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Acme")
    .groupBy(
        "supplier_id",
        F.quarter("launch_date").alias("launch_quarter"),
        "category",
    )
    .agg(F.count("cost").alias("cost_count"))
)
