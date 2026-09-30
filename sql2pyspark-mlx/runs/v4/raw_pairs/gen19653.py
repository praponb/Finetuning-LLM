from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(
        F.when(F.year("launch_date") < 2024, "before_2024").when(F.year("launch_date") == 2024, "2024").otherwise("after_2024").alias("launch_cohort"),
        "brand",
    )
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.sum(F.when(F.col("brand") == "Acme", 1).otherwise(0)).alias("acme_count"),
    )
    .filter(F.col("acme_count") > 27)
)
