from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("region").isin(["North", "East"]) & (F.col("opened_date") <= "2023-05-01"))
    .agg(
        F.sum(F.when(F.col("city") == "London", F.col("sqft")).otherwise(0)).alias("london_sqft"),
    )
    .orderBy("london_sqft")
)
