import os
import pandas as pd
from pandasai import Agent
from pandasai import SmartDataframe
from pandasai.llm import Anthropic

os.environ["PANDASAI_API_KEY"] = "$2a$10$wqRNlp9vws.J2xj7Ecb/eObCJlH3W.KhNi4qbZH7wdM.txkhTgjNm"

df = pd.read_csv("finance-2024-combined.csv")
agent=Agent(df)

#agent.chat("plot histogram of categories")
#agent.chat("plot histogram of categories in normal distribution")


#agent.chat("take the top category in terms of absolute sum and plot the absolute value by month")

#agent.chat("take the second highest category in terms of absolute sum and plot the absolute value by month")

#agent.chat("take the second highest category in terms of absolute sum and plot the absolute value by month along with the trend line in red")

#agent.chat("show the most interesting trend in the dataset")

#agent.chat("filter out salary and rental income, aggregate everything else as expense, plot horizontal bar chart month over month")

sdf=SmartDataframe(df,config={"verbose": True})
#result = sdf.chat("disregarding 'Monthly property expense', what's the top 5 most negative items?")
#print(result)
#df_category_mean = sdf.chat("what's the median value of the monthly sum for each category? order by decreasing value")
#print(df_category_mean)
#df_spendings = sdf.chat("remove rows with positive amounts")
#agent = Agent(df_spendings)
#agent.chat("plot group bar chart with amount total by categories by month")
#agent.chat("now retain only the bottom 6 categories")

df = sdf.chat("show pairs of rows whose values add up to exactly 0")
print(df)
