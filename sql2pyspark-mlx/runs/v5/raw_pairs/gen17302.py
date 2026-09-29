from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("opened_date") <= "2025-09-23") | (F.year("opened_date") >= 2025))
    .select(
        "id",
        "city",
        F.when(F.col("sqft") >= 16267, "High").when(F.col("sqft") >= 2972, "Medium").otherwise("Low").alias("sqft_band"),
    )
)
