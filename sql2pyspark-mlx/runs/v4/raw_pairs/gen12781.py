from pyspark.sql import functions as F

result = (
    df_products
    .filter((F.col("cost") < 440) & (F.col("cost") <= 510))
    .select(
        "launch_date",
        F.date_format("launch_date", "yyyy-MM").alias("launch_date_label"),
        F.when(F.col("price") >= 1180, "High").when(F.col("price") >= 350, "Medium").otherwise("Low").alias("price_level"),
    )
    .orderBy("launch_date")
)
