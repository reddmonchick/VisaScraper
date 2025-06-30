from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import os

def upload_to_drive(pdf_content: bytes, filename: str, folder_id: str = None) -> str:
    """
    Загружает PDF-файл в Google Drive или возвращает существующий,
    если файл с таким именем уже есть. Возвращает публичную ссылку.
    """

    # Авторизация через Service Account
    gauth = GoogleAuth(settings_file='src/drive.yaml')
    gauth.ServiceAuth()
    drive = GoogleDrive(gauth)

    # Проверяем, существует ли уже файл с таким именем
    query_parts = [f"title = '{filename}'"]
    if folder_id:
        query_parts.append(f"'{folder_id}' in parents")
    else:
        query_parts.append("trashed=false")  # исключаем удалённые

    query = " and ".join(query_parts)
    file_list = drive.ListFile({'q': query}).GetList()

    if file_list:
        # Файл уже существует — возвращаем первую найденную ссылку
        existing_file = file_list[0]
        #print(f"Файл уже существует: {existing_file['alternateLink']}")
        return existing_file['alternateLink']

    # Сохраняем временно PDF
    temp_dir = "src/temp"
    os.makedirs(temp_dir, exist_ok=True)
    file_path = os.path.join(temp_dir, filename)

    with open(file_path, "wb") as f:
        f.write(pdf_content)

    # Метаданные файла
    file_metadata = {
        'title': filename,
        'mimeType': 'application/pdf',
    }

    if folder_id:
        file_metadata['parents'] = [{'id': folder_id}]

    file_drive = drive.CreateFile(file_metadata)
    file_drive.SetContentFile(file_path)
    file_drive.Upload(param={'supportsAllDrives': True})

    # Делаем файл общедоступным
    file_drive.InsertPermission({
        'type': 'anyone',
        'value': 'anyone',
        'role': 'reader'
    })

    public_link = file_drive['alternateLink']
    #print(f"Новый файл загружен: {public_link}")
    return public_link