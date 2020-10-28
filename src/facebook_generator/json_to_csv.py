import json
import sys
import pandas as pd

def main():
    read_file = sys.argv[1]
    row_list = []
    with open(read_file) as file:
        data = json.load(file)

        for object in data:
            for team in object["favorite_teams"]:
                row = dict()
                row["name"] = object["name"]
                row["id"] = object["id"]
                row["favorite_team"] = team["name"]
                row["favorite_team_id"] = team["id"]
                row["favorite_athlete"] = "null"
                row["favorite_athlete_id"] = "null"
                row_list.append(row)
            for athlete in object["favorite_athletes"]:
                row = dict()
                row["name"] = object["name"]
                row["id"] = object["id"]
                row["favorite_team"] = "null"
                row["favorite_team_id"] = "null"
                row["favorite_athlete"] = athlete["name"]
                row["favorite_athlete_id"] = athlete["id"]
                row_list.append(row)
        df = pd.DataFrame(row_list)
        df.to_csv("data.csv", index=False)

if __name__ == "__main__":
    main()