from pyspark.sql import functions as F

result = df_subscriptions.select("plan", "start_date")
