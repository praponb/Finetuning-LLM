from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(~(F.col("store_id").isin([1, 6]) & F.col("store_id").isin([4, 5, 6])))
    .select(
        "id",
        "store_id",
        "units",
        F.weekofyear("sale_date").alias("sale_date_weekofyear"),
    )
    .orderBy("sale_date_weekofyear")
)
