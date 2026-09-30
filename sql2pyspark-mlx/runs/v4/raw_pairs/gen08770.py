from pyspark.sql import functions as F

avg_rating = df_suppliers.filter(~F.col("country").isin(["TH", "US", "SG"])).agg(F.avg("rating")).first()[0]

result = (
    df_suppliers
    .filter(F.col("rating") >= avg_rating)
    .select("id", "rating")
)
