from zipfile import ZipFile

with ZipFile('data/log-data.zip', 'r') as zip_ref:
	zip_ref.extractall('data/log-data-unzipped')

with ZipFile('data/song-data.zip', 'r') as zip_ref:
	zip_ref.extractall('data/song-data-unzipped')