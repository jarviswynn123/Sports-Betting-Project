# Title
DE101 Bootcamp- Code Review Week 10

# Project Members
Nikisha Banks, Jarvis Wynn, Alex Wallace

# Technologies Used: 
Git hub, Visual Studio Code, Google Cloud, Google Looker Studio

# Languages and tools used: 
Python, Pandas, Kaggle, SQL

# Kaggle Datasets Used
 https://www.kaggle.com/datasets/tobycrabtree/nfl-scores-and-betting-data

# Description:
In this project, we have created a data pipeline that brings data from Kaggle, transforms it in Visual Studio and then stores it as datasets in Google Big Query. Once it is stored, several visualizations were created to show various metrics on NFL betting.

First, we constructed an ERD (entity relation diagram) depicting the organizational structure we wanted our dataset to have. This ERD can be seen here:

![ERD](https://github.com/thehalfkoreanzombie/Team_Week/blob/main/images/nfl_sports_betting.drawio.png)

In this structure, we ran all tables through a dimensional table called dim_games. This table has all of the general info regarding each nfl game played from 1979-2023. From this table, we created two fact tables. the fct_over_under table contains team and score info, along with the over/under line for each game. We created a column called total_score to determine whether or not each game went over or under the money line. We then created a column called "over" that gave a True or False depending on whether the total_score went over the over/under money line score for the game. We also created a fact table called fct_spread, which had team and score info for each game, the team_id for the team that was the favorite, and the spread for that game. 

To give a brief explanation, NFL games are typically betted on using one of two numbers: the over/under line or the game's spread. The over/under line represents the total amount of points bookies expect the aggregate score of a game to be. People can bet on whether they think the score will be over or under that number. If the over/under line is 50 points, and both teams score a total of 51 or more points, then whoever bet on the 'over' wins their bet. If both teams score 49 or less points, the people who bet on the 'under' wins their bet. If the score is exactly 50, that is called a 'push' and everyone gets their money back. We did not account for pushes in our 'over' column, but that could be an improvement we could make in the future for this project.

The spread of a game is determined by how many points a team is expected to beat another team by. Typically, spreads are depicted by positive and negative numbers, with negative numbers depicting how much a favored team is supposed to win by, and positive numbers depicting how much an underdog team is supposed to lose by. People can bet on either team in a 'spread' bet. Lets say, for example, the chiefs have a -6.5 spread against the bears. If someone were to bet on the chief's spread, the chiefs would have to win by 6.5 points or more for them to win their bet. If someone bets on the bears in this situation, the bears are allowed to lose by up to 6.5 points in order for that person to win their bet.

We had to make a couple of transformations to our dataframe before loading it into a dataset. We removed all games before 1979, because before then, there were very few betting numbers as sports betting wasn't legal until they legalized it in Atlantic City in late 1978. We also removed a couple of games after Christmas of 2023 as they had null values for the betting columns (they likely hadn't been updated past that point). Lastly, we made sure each column was the necessary datatype and loaded our tables into bigquery using a python bigquery client and ETL (extract, transform, load) pipeline. 

Nikisha also created a table containing the name of each team in comparison to their team id, as we only had the team id in each of the fact tables. This way, users can determine which team was the favorite if they weren't sure what the team id for a team was. 

Using looker studio, we all created a visualization page depicting answers to certain questions we had that could be found using our organized dataset. Jarvis created a bar graph showing how many times the favorite in a superbowl won the game. Nikisha created some visualizations showing how many times games went over or under in a year, as well as how many times the home team or away team won each year. Alex created a visualization page on the Detroit Lions team, showing overall game and betting data for this particular team. All of these visuals can be found using the link [Looker Studio NFL Sports Betting](https://lookerstudio.google.com/reporting/dc08b504-e8d6-4cac-b48e-af466d8bb2cf/page/p_kdqk1zbidd)

We plan on inserting images of each page into the README in the future, but have not figured out how to export specific pages as image files just yet. Hopefully that will happen soon.

# Setup/Installation Requirements:
To run the code in this project:
1. Clone the repo in Git Hub: 
   a. Click the green "code" button
   b. Under the "Local" tab, copy and paste the URL that is under HTTPS
2.  
3. In the terminal, activate the virtual environment by typing 
        python3.10 -m venv venv
        source venv/bin/activate
4. Install the requirements from the requirements.txt file by typing:
        pip install -r requirements.txt
4.  Download the data file from Kaggle at : 

# Known Bugs
No known bugs

# ERD model
![Image](https://github.com/thehalfkoreanzombie/team-week-10/blob/main/images/team_week_10.drawio.png)

# Project Visuals
## Big Query Datasets

**Games Dimension Table Schema**
![Image](https://github.com/thehalfkoreanzombie/team-week-10/blob/main/images/Games_Dimension_Tble.png)
---
**Over/Under Fact Table Schema**
![Image](https://github.com/thehalfkoreanzombie/team-week-10/blob/main/images/Ovr_Undr_FctTble.png)

# License
*Copyright 2024, Data Stack Academy Fall 2023 Cohort*

*Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:*

*The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.*

*THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.*
# BootCampCodeReview10
