import requests
import tempfile
import os
import csv


def _get_single_address(address):
    url = 'https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress'
    data = {
        'benchmark': 'Public_AR_Current',
        'vintage': 'Current_Current',
        'format': 'json',
        'address': address
    }

    response = requests.post(url, data=data)

    data = response.json()

    return data['result']['addressMatches']


def _break_address_ties(outputpath):
    with open(outputpath, 'r') as f:
        reader = csv.reader(f)
        all_lines = list(reader)

    num = 0
    for line in all_lines:
        if line[2] in ['No_Match', 'Tie']:
            res = _get_single_address(line[1])
            if len(res) > 0:
                num = num + 1
                line[2] = 'Match'
                line[3] = 'Exact'
                line[4] = res[0]['matchedAddress']
                line[5] = f'{res[0]['coordinates']['x']},{res[0]['coordinates']['y']}'
                line[6] = res[0]['tigerLine']['tigerLineId']
                line[7] = res[0]['tigerLine']['side']
                
                for k in res[0]['geographies'].keys():
                    if 'Census Block' in k:
                        block_obj = res[0]['geographies'][k][0]
                        line[8] = block_obj['STATE']
                        line[9] = block_obj['COUNTY']
                        line[10] = block_obj['TRACT']
                        line[11] = block_obj['BLOCK']
                        break
    
    with open(outputpath, 'w') as outfile:
        writer = csv.writer(outfile)
        writer.writerows(all_lines) 

    print(f'Broke {num} ties')


def get_census_geocode_batch_results(
        inputpath : str,
        outputpath : str,
        header : bool = True,
        chunksize : int = 5000,
        id_col_name : str = 'id',
        append : bool = False,
        breakties : bool = False
):
    
    col_titles = [
        id_col_name,
        "address",
        "match",
        "matchtype",
        "parsed",
        "coordinate",
        "tigerlineid",
        "side",
        "statefp",
        "countyfp",
        "tract",
        "block",
    ]

    url = "https://geocoding.geo.census.gov/geocoder/geographies/addressbatch"
    data = {
        'benchmark': 'Public_AR_Current',
        'vintage': 'Current_Current',
    }

    with open(inputpath) as f:
        addresses = f.readlines()

    start_row = int(header)
    total_rows = len(addresses) - start_row

    if not append:
        with open(outputpath, "w") as f:
            f.write(','.join(col_titles) + '\n')

    with tempfile.TemporaryDirectory() as dirname:
        end_row = start_row + chunksize
        while start_row < total_rows:
            chunk = addresses[start_row:end_row]

            filepath = os.path.join(dirname, f'file{start_row}.csv')
            with open(filepath, 'w') as f:
                f.writelines(chunk)

            files = {
                'addressFile': (filepath, open(filepath, 'rb'), 'text/csv')
            }

            response = requests.post(url, data=data, files=files)

            with open(outputpath, "ab") as f:
                f.write(response.content)

            print(f'{min([end_row - 1, total_rows])} of {total_rows} complete')

            start_row = end_row
            end_row = end_row + chunksize

    if breakties:
        print('Breaking ties...')
        _break_address_ties(outputpath)

    
    print(f'Geocoded addresses outputed: {outputpath}')


