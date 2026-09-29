from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy("channel")
    .pivot("promo_code", ["SPRING10", "VIP", "WELCOME5"])
    .agg(F.max("discount_pct"))
)
