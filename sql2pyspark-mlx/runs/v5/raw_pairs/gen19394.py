from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Initech", "Stark", "Umbrella"]))
    .select(F.lit("product").alias("record_type"), "price")
    .unionByName(
        df_payments
        .filter(F.col("method") == "bank_transfer")
        .select(F.lit("payment").alias("record_type"), F.col("amount").alias("price"))
    )
    .distinct()
    .orderBy("price")
)
