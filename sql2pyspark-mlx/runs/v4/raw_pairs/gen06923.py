from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") <= "2024-08-31")
    .groupBy(
        F.when(F.year("last_restock") < 2024, "before_2024").when(F.year("last_restock") == 2024, "2024").otherwise("after_2024").alias("last_restock_cohort"),
        "store_id",
    )
    .agg(F.count("*").alias("cnt"))
    .filter(F.col("cnt") >= 9)
    .select("last_restock_cohort", "store_id", "cnt")
    .orderBy(F.col("cnt").desc())
)
