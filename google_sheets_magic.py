import gspread
from oauth2client.service_account import ServiceAccountCredentials
import math
#------------------------
#------------------------Google sheets
#------------------------
scope = ['https://spreadsheets.google.com/feeds','https://www.googleapis.com/auth/drive']
client = None

sheets = {}
spreadsheets = {}

def init():
    """read credentials in the GSheetsCredentials.json file, and init the connection """
    global client,sheets,spreadsheets
    sheets = {}
    spreadsheets = {}
    creds = ServiceAccountCredentials.from_json_keyfile_name('GSheetsCredentials.json', scope)
    client = gspread.authorize(creds)
init()

def _getSheet(spreadsheet, sheetname):
    """cache system for tabs"""
    if(not sheetname in sheets):
        sheets[sheetname] = spreadsheet.worksheet(sheetname)
    return sheets[sheetname]

#système de cache pour les spreadsheets
def _getSpreadsheet(spreadsheetname):
    """cache system for spreadsheets"""
    if(not spreadsheetname in spreadsheets):
        spreadsheets[spreadsheetname] = client.open(spreadsheetname)
    return spreadsheets[spreadsheetname]

#injecte un tableau 2D dans un onglet d'une spreadsheet
def _exp2D(tab, sheet, cell):
    """send a 2D array to a specific cell in a specific sheet"""
    (r,c) = gspread.utils.a1_to_rowcol(cell)
    nbr = len(tab)
    nbc = len(tab[0])
    _range = sheet.range(r, c, r+nbr-1, c+nbc-1)
    for i,cell in enumerate(_range):
        cell.value = tab [math.floor(i/nbc)] [i%nbc]
    sheet.update_cells(_range)

#------------ out
def send(spreadsheet, sheet, cell, tab):
    """send a 2D array to a specific cell in a specific sheet (by sheet name) in a specific spreadsheet (by spreadsheet name)"""
    _exp2D(tab,_getSheet(_getSpreadsheet(spreadsheet),sheet),cell)

def getAll(spreadsheet, sheet):
    """get all data of a specific sheet (by sheet name) in a specific spreadsheet (by spreadsheet name)"""
    try:
        return _getSheet(_getSpreadsheet(spreadsheet),sheet).get_all_values()
    except :
        init()
        return _getSheet(_getSpreadsheet(spreadsheet),sheet).get_all_values()

def getCell(spreadsheet, sheet, cell):
    return _getSheet(_getSpreadsheet(spreadsheet),sheet).acell(cell).value

def getRow(spreadsheet, sheet, row):
    return _getSheet(_getSpreadsheet(spreadsheet),sheet).row_values(row)

def getCol(spreadsheet, sheet, col):
    return _getSheet(_getSpreadsheet(spreadsheet),sheet).col_values(col)

def clear(spreadsheet, sheet):
    """delete all data in a specific sheet (by sheet name) in a specific spreadsheet (by spreadsheet name)"""
    all = getAll(spreadsheet, sheet)
    if len(all)==0 :
        return None
    for i,row in enumerate(all):
        for j,cell in enumerate(row):
            all[i][j] = ' '
    send(spreadsheet, sheet, 'A1', all)


