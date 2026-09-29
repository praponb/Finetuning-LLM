from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-04-01", "2024-02-29") & F.col("segment").isin(["Consumer", "SMB", "Corporate"]))
    .groupBy("segment", "city")
    .agg(
        F.countDistinct("credit_limit").alias("unique_citys"),
        F.countDistinct("age").alias("unique_countrys"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy("unique_citys")
)
