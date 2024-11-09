import os
from crewai import Agent, Task, Crew
from crewai_tools import SerperDevTool, FileReadTool
from langchain_community.llms import Ollama
import csv
os.environ["SERPER_API_KEY"] = "2b9561b3b09dc9311e122a8da561d224f7cbe15b"
# Get this from https://serper.dev/playground

llm = Ollama(model="llama2")

search_tool = SerperDevTool()
# file_read_tool = FileReadTool(file_path="domain3test.csv")
# read csv file
file_content = csv.reader(open('agent/domain3test.csv'), delimiter=',', quotechar='"')
rows = []
for row in file_content:
    rows.append(row)

researcher = Agent(
    llm=llm,
    role="AWS Data Engineer",
    goal="Output a file with rewritten questions and answers from a CSV file.",
    backstory="You are a AWS data engineer who has to review questions and answers from input and find the questions that are not clearly worded. You need to rewrite the questions in a clear and concise manner.",
    allow_delegation=False,
    # tools=[file_read_tool],
    verbose=True,
)

task1 = Task(
    description=f"Review and rewrite questions and answers from {rows}. The questions and some answers are not clearly worded and need to be rewritten clearly. If the question is clear, leave it as is.",
    expected_output=""" Write the output to a file called task1_output.txt. The output should contain the rewritten questions and answers in the same format as the input.""",
    agent=researcher,
    output_file="task1_output.txt",
)


crew = Crew(agents=[researcher], tasks=[task1], verbose=2)

task_output = crew.kickoff()
print(task_output)