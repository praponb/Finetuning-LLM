from pyspark.sql import functions as F

result = (
    df_products
    .select("launch_date")
    .intersect(
        df_subscriptions
        .filter(~F.col("plan").isin(["basic", "enterprise", "standard"]))
        .select("start_date")
    )
)
