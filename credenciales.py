from oauth2client.service_account import ServiceAccountCredentials
import gspread

json = 'key.json'
scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
creds = ServiceAccountCredentials.from_json_keyfile_name(json, scope)
gc = gspread.authorize(creds)
token='5704193659:AAFer0j_h92p5EOky9bIgXQypF8cPi9Mtxo'
key = '1WQdSUhEuNfiGWhTguH-ulQTfDeNTMs5UMuTIgf1VUp0'
key_query = '1Rq6Kf2SxoPv6JFX1VDoTOnRJGoad89wUWlTlvUy0PFs'
