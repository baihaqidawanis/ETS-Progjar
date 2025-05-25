import os
import base64
import json
from glob import glob

class FileInterface:
    def __init__(self):
        self.folder = 'files'
        if not os.path.isdir(self.folder):
            os.mkdir(self.folder)
        os.chdir(self.folder)

    def list(self, params=[]):
        try:
            daftar = glob("*.*")
            return {'status': 'OK', 'data': daftar}
        except Exception as e:
            return {'status': 'ERROR', 'data': str(e)}

    def get(self, params=[]):
        try:
            nama = params[0]
            if not nama:
                return {'status': 'ERROR', 'data': 'Nama file kosong'}
            with open(nama, 'rb') as f:
                encoded = base64.b64encode(f.read()).decode()
            return {'status': 'OK', 'data_namafile': nama, 'data_file': encoded}
        except Exception as e:
            return {'status': 'ERROR', 'data': str(e)}

    def upload(self, params=[]):
        try:
            nama_file = params[0]
            isi_encoded = params[1]
            isi_bytes = base64.b64decode(isi_encoded)
            with open(nama_file, 'wb') as f:
                f.write(isi_bytes)
            return {'status': 'OK', 'data': f'File {nama_file} berhasil disimpan'}
        except Exception as e:
            return {'status': 'ERROR', 'data': str(e)}

    def delete(self, params=[]):
        try:
            target = params[0]
            if os.path.exists(target):
                os.remove(target)
                return {'status': 'OK', 'data': f'File {target} berhasil dihapus'}
            else:
                return {'status': 'ERROR', 'data': 'File tidak ditemukan'}
        except Exception as e:
            return {'status': 'ERROR', 'data': str(e)}


if __name__ == '__main__':
    f = FileInterface()
    print(f.list())
    print(f.get(['pokijan.jpg']))
    
    contoh_data = base64.b64encode(b'Teks dari client untuk server').decode()
    print(f.upload(['upload_tes.jpg', contoh_data]))
    print(f.delete(['upload_tes.jpg']))
