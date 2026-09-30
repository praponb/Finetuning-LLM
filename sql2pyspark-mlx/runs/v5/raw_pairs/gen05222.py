from pyspark.sql import functions as F

result = df_subscriptions.filter(~F.col("plan").isin(["standard", "basic"]))
