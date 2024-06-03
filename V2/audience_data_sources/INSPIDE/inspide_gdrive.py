import gdown

# Lista de URLs de los archivos de Google Drive que quieres descargar
file_urls = [
    'https://drive.google.com/uc?id=file_id1',
    'https://drive.google.com/uc?id=file_id2',
    # Añade más URLs según sea necesario
]

for file_url in file_urls:
    gdown.download(file_url, quiet=False)


