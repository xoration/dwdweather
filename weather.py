import urllib.request
import zipfile
import csv
import os, fnmatch 
import psycopg
import datetime
import argparse


parser = argparse.ArgumentParser(description='Weather download script.')

parser.add_argument('--db', required=True,
                    help='dbname')
parser.add_argument('--username', required=True,
                    help='Username')
parser.add_argument('--password', required=True,
                    help='Password')
group = parser.add_mutually_exclusive_group(required=True)
group.add_argument('--url', 
                    help='URL to download weather data')
group.add_argument('--station', type=int, choices=range(1, 100000),
                    help='Stationid to catch')
group.add_argument('--csv', 
                    help='csv with weather data')


args = parser.parse_args()
print(args) 
insertSql = "insert into weatherdata (stations_id, mess_datum, qn_3, fx, fm, qn_4, rsk, rskf, sdk, shk_tag, nm, vpm, pm, tmk, upm, txk, tnk, tgk) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"

if not args.csv:
    if args.url:
        weatherUrl = args.url
        
    if args.station:
        station = str(args.station).zfill(5)

        weatherUrl = 'https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/daily/kl/recent/tageswerte_KL_' + station + '_akt.zip'



    def findCSV():
        for root, dirs, files in os.walk('data'):
            for name in files:
                if fnmatch.fnmatch(name, 'produkt_klima*'):
                    return name




    print('Beginning file download...')
    urllib.request.urlretrieve(weatherUrl, 'tageswerte.zip')


    with zipfile.ZipFile('tageswerte.zip', 'r') as zip_ref:
        zip_ref.extractall('data')


    
    csvName = findCSV()
    path = 'data/' + csvName
else:
    path = args.csv

rowsAdded=0
connectString = "dbname=" + args.db + " " + "user=" + args.username + " " + "password=" + args.password

with psycopg.connect(connectString) as conn:
    with conn.cursor() as cur:

        lastDate = datetime.date(1900,1,1)

        if not args.csv:
            cur.execute('select max(mess_datum) from weatherdata;')
            lastDate = cur.fetchone()

            if lastDate[0] is not None:
                lastDate = lastDate[0]
            

        with open(path) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=';')
            line_count = 0
            for row in csv_reader:
                if line_count == 0:
                    line_count += 1
                    continue
                
                date = datetime.datetime.strptime(row[1], "%Y%m%d")
                if date.date() > lastDate:
                    # insert into db
                    val = (row[0].strip(), date.strftime('%Y-%m-%d %H:%M:%S'), row[2].strip(), row[3].strip(), row[4].strip(), row[5].strip(), row[6].strip(), row[7].strip(), row[8].strip(), row[9].strip(), row[10].strip(), row[11].strip(), row[12].strip(), row[13].strip(), row[14].strip(), row[15].strip(), row[16].strip(), row[17].strip())
                    cur.execute(insertSql, val)
                    rowsAdded += 1
                    
                line_count += 1
    conn.commit()


print("Added: " + str(rowsAdded) + " datasets.")
    

if not args.csv:
    print("Cleanup")
    os.remove('tageswerte.zip')
    for root, dirs, files in os.walk('data'):
        for name in files:
            os.remove(os.path.join(root, name))
    
    os.removedirs('data')
