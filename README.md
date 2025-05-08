# bluesky-scraper
Grabs BlueSky posts via API, classifies them based on Controversial score via openai API and then inserts them into Google sheets via sheets API

Google sheets template - https://docs.google.com/spreadsheets/d/1Jq2dA7lKcncI8co5-zM9kQJFRbkk2jfB3iFwBCakA1A/edit?usp=sharing

Input your Bluesky, openai credentials and the google sheeet ID/name into config.json.example and then rename it to config.json

Input your google sheets project API credentials into credentials.json.example and rename it to credentials.json

Add a new line delimited list of bluesky users you want to scrape to user_list.txt
