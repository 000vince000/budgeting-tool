import os
import pandas as pd
from pandasai import Agent

os.environ["PANDASAI_API_KEY"] = "$2a$10$wqRNlp9vws.J2xj7Ecb/eObCJlH3W.KhNi4qbZH7wdM.txkhTgjNm"

df = pd.read_csv("finance-2024-combined.csv")
agent=Agent(df)

#agent.chat("plot histogram of categories")

agent.chat("plot histogram of categories in normal distribution")

agent.chat("plot sorted absolute value of amount total in log scale by categories with different colors for July, with total shown in each category")

#agent.chat("take the top category in terms of absolute sum and plot the absolute value by month")

#agent.chat("take the second highest category in terms of absolute sum and plot the absolute value by month")

#agent.chat("take the second highest category in terms of absolute sum and plot the absolute value by month along with the trend line in red")

#agent.chat("show the most interesting trend in the dataset")

