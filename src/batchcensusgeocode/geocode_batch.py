import requests
import tempfile
import os


def get_census_geocode_batch_results(
        inputpath : str,
        outputpath : str,
        header : bool = True,
        chunksize : int = 5000
):
    header = True
    chunksize = 30
    url = "https://geocoding.geo.census.gov/geocoder/geographies/addressbatch"
    data = {
        'benchmark': 'Public_AR_Current',
        'vintage': 'Current_Current',
    }

    with open(inputpath) as f:
        addresses = f.readlines()

    start_row = int(header)
    total_rows = len(addresses) - start_row
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


