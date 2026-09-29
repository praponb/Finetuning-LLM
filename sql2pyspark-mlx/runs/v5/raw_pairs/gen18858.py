from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-03-16")))
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
)

result = (
    base
    .select("id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("product_id").isin([1, 2]))
        .select("id", "store_id")
    )
)
