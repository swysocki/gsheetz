import json
from operator import itemgetter
from itertools import groupby
from googleapiclient import discovery
from google.oauth2 import service_account

CREDENTIALS_FILE = "eco-groove-150118-d033e728779d.json"
FILENAME = "tournament_data.json"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
SHEET_ID = "1yNN5YVlD9Eb0H1P54T3DpQNzAojUnSGCXD83_Tn23uU"


def get_data_fields(fields):
    """ Returns the data field indexes"""
    field_map = {
        "year": fields.index("Year"),
        "location": fields.index("Location"),
        "name": fields.index("Name"),
        "division": fields.index("Division"),
        "format": fields.index("Format"),
        "team": fields.index("Team"),
        "place": fields.index("Place"),
    }
    tourn_fields = itemgetter(
        field_map.get("year"), field_map.get("location"), field_map.get("name")
    )
    result_fields = itemgetter(
        field_map.get("division"),
        field_map.get("format"),
        field_map.get("team"),
        field_map.get("place"),
    )

    return tourn_fields, result_fields


def aggregate_results(tourn_list, sheet_name):
    """
    Group list by year, location, name, division, format
    Assume the items are sorted so that the whole list will not be iterated for
    each group

    {
      'year': 2016,
      'location': 'New York, NY',
      'name': 'My Tourn',
      'league': $sheet_name,
      'results': [
        {
          'format':'10-man',
          'division': 'Amateur',
          'team': 'My Team',
          'place': '1'
        }
      ]
    }
    """
    field_list = tourn_list.pop(0)
    t_getter, p_getter = get_data_fields(field_list)

    t_list = []
    for g1, v1 in groupby(tourn_list, t_getter):
        tourn = {"year": g1[0], "location": g1[1], "name": g1[2], "league": sheet_name, "results": []}
        for g2, v2 in groupby(v1, p_getter):
            tourn["results"].append(
                {"division": g2[0], "format": g2[1], "team": g2[2], "place": g2[3]}
            )
        t_list.append(tourn)
    return t_list


def get_sheet_data(service, sheet_info):
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=SHEET_ID, range=f"{sheet_info[0]}")
        .execute()
    )
    return result


def get_spreadsheet_title(service):
    """ Returns a tuple of title, id"""

    result = service.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    spreadsheet_meta = result.get("sheets", "")
    sheet_list = []
    for sheet_title in spreadsheet_meta:
        sheet_list.append(
            (
                sheet_title.get("properties").get("title"),
                sheet_title.get("properties").get("sheetId"),
            )
        )
    return sheet_list


if __name__ == "__main__":
    credentials = service_account.Credentials.from_service_account_file(
        CREDENTIALS_FILE, scopes=SCOPES
    )
    service = discovery.build("sheets", "v4", credentials=credentials)
    sheet_ids = get_spreadsheet_title(service)
    tourn_list = []
    for sheet in sheet_ids:
        tournamentd_data = get_sheet_data(service, sheet)
        tournament_list = aggregate_results(tournamentd_data.get("values"), sheet[0])
        tourn_list.extend(tournament_list)
    with open(FILENAME, "w") as df:
        json.dump(tourn_list, df)
