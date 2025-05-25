import json
import logging
import shlex
import base64

from file_interface import FileInterface

"""
Class FileProtocol digunakan untuk menangani permintaan dari client.
Input berupa string perintah, dan akan diproses untuk menentukan aksi
yang sesuai menggunakan FileInterface.
"""

class FileProtocol:
    def __init__(self):
        self.file = FileInterface()

    def proses_string(self, string_datamasuk=''):
        logging.warning(f"data masuk: {string_datamasuk}")

        # Tangani perintah khusus yang memiliki format khusus
        if string_datamasuk.startswith("UPLOAD "):
            return self._handle_upload(string_datamasuk)

        if string_datamasuk.startswith("DELETE "):
            return self._handle_delete(string_datamasuk)

        # Perintah umum seperti LIST dan GET
        try:
            tokens = shlex.split(string_datamasuk)
            if not tokens:
                return json.dumps({'status': 'ERROR', 'data': 'Perintah kosong'})
            
            perintah = tokens[0].lower()
            argumen = tokens[1:] if len(tokens) > 1 else []

            if hasattr(self.file, perintah):
                fungsi = getattr(self.file, perintah)
                hasil = fungsi(argumen)
                return json.dumps(hasil)
            else:
                return json.dumps({'status': 'ERROR', 'data': 'Perintah tidak dikenali'})
        except Exception as e:
            return json.dumps({'status': 'ERROR', 'data': str(e)})

    def _handle_upload(self, string_input):
        try:
            data = string_input.strip().split(" ", 1)[1]
            if "||" not in data:
                return json.dumps({'status': 'ERROR', 'data': 'Format upload salah'})
            nama_file, b64_konten = data.split("||", 1)
            nama_file = nama_file.strip()
            b64_konten = b64_konten.strip()

            return json.dumps(self.file.upload([nama_file, b64_konten]))
        except Exception as e:
            return json.dumps({'status': 'ERROR', 'data': f'Upload gagal: {str(e)}'})

    def _handle_delete(self, string_input):
        try:
            data = string_input.strip().split(" ", 1)
            if len(data) < 2 or data[1].strip() == '':
                return json.dumps({'status': 'ERROR', 'data': 'Nama file harus disertakan'})
            target = data[1].strip()
            return json.dumps(self.file.delete([target]))
        except Exception as e:
            return json.dumps({'status': 'ERROR', 'data': f'Delete gagal: {str(e)}'})


if __name__ == '__main__':
    fp = FileProtocol()
    print(fp.proses_string("LIST"))
    print(fp.proses_string("GET pokijan.jpg"))
