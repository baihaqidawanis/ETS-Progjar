import socket
import json
import base64
import logging
import os
import time
import argparse
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed

SERVER_ADDRESS = ('172.16.16.101', 7777)
STRESS_TEST_FILES = {
    '10MB': '10MB.dat',
    '50MB': '50MB.dat',
    '100MB': '100MB.dat'
}

def send_command(command_str=""):
    """Mengirim perintah ke server dan menerima respon JSON."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect(SERVER_ADDRESS) 
        logging.warning(f"Terhubung ke {SERVER_ADDRESS}")
        command_str += "\r\n\r\n"
        sock.sendall(command_str.encode())

        data_received = ""
        while True:
            data = sock.recv(65536)
            if data:
                data_received += data.decode()
                if "\r\n\r\n" in data_received:
                    break
            else:
                break

        hasil = json.loads(data_received)
        return hasil
    except Exception as e:
        logging.warning(f"Gagal menerima data: {e}")
        return {"status": "ERROR", "data": str(e)}
    finally:
        sock.close()

def remote_list():
    hasil = send_command("LIST")
    if hasil.get('status') == 'OK':
        print(" Daftar file yang tersedia:")
        for file in hasil['data']:
            print(f"- {file}")
        return True
    else:
        print("Gagal mengambil daftar file.")
        return False


def remote_get(filename=""):
    start_time = time.time()
    hasil = send_command(f"GET {filename}")
    elapsed_time = time.time() - start_time
    if hasil and hasil.get('status') == 'OK':
        try:
            nama_file = hasil['data_namafile']
            with open(hasil['data_namafile'], 'wb') as fp:
                fp.write(base64.b64decode(hasil['data_file']))
            print(f" File '{filename}' berhasil diunduh.")
            return {
                "status": "OK",
                "filename": nama_file,
                "time": elapsed_time,
                "size": os.path.getsize(nama_file),
                "error": None
            }
        except Exception as e:
            print(f" Kesalahan saat menyimpan file: {e}")
            return {
                'status': 'ERROR',
                'filename': filename,
                'size': 0,
                'time': elapsed_time,
                'error': str(e)
            }
    else:
        print(" Gagal mengunduh file.")
        return {
            'status': 'ERROR',
            'filename': filename,
            'size': 0,
            'time': elapsed_time,
            'error': hasil.get('data', 'Unknown error') if hasil else 'No response'
        }


def remote_upload(filepath="", upload_as=""):
    if not os.path.exists(filepath):
        print(f" File '{filepath}' tidak ditemukan.")
        return False

    start_time = time.time()
    try:
        with open(filepath, 'rb') as f:
            encoded_data = base64.b64encode(f.read()).decode()

        filename = upload_as or os.path.basename(filepath)
        command = f"UPLOAD {filename}||{encoded_data}"
        hasil = send_command(command)
        elapsed_time = time.time() - start_time

        if hasil.get('status') == 'OK':
            print(f" File '{filename}' berhasil di-upload.")
            return {
                'status': 'OK',
                'filename': filename,
                'size': os.path.getsize(filepath),
                'time': elapsed_time,
                'error': None
            }
        else:
            print(f" Gagal upload: {hasil.get('data')}")
            return {
                'status': 'ERROR',
                'filename': filename,
                'size': 0,
                'time': elapsed_time,
                'error': hasil.get('data', 'Unknown error')
            }
    except Exception as e:
        elapsed_time = time.time() - start_time
        print(f" Kesalahan saat upload: {e}")
        return {
            'status': 'ERROR',
            'filename': filepath,
            'size': 0,
            'time': elapsed_time,
            'error': str(e)
        }

def remote_delete(filename=""):
    hasil = send_command(f"DELETE {filename}")
    if hasil.get('status') == 'OK':
        print(f" File '{filename}' berhasil dihapus dari server.")
        return True
    else:
        print(f" Gagal menghapus file: {hasil.get('data')}")
        return False

def stress_test_worker(operation, filename):
    if operation == 'upload':
        return remote_upload(filename)
    elif operation == 'download':
        return remote_get(filename)
    else:
        return {
            'status': 'ERROR',
            'filename': filename,
            'size': 0,
            'time': 0,
            'error': 'Unknown operation'
        }

def run_stress_test(operation, size_label, client_pool_size):
    filename = STRESS_TEST_FILES[size_label]
    results = []
    with ThreadPoolExecutor(max_workers=client_pool_size) as executor:
        futures = []
        for _ in range(client_pool_size):
            if operation == 'upload':
                futures.append(executor.submit(stress_test_worker, 'upload', filename))
            elif operation == 'download':
                futures.append(executor.submit(stress_test_worker, 'download', filename))
        for future in as_completed(futures):
            try:
                result = future.result()
                if not isinstance(result, dict):
                    result = {
                        'status': 'ERROR',
                        'filename': filename,
                        'size': 0,
                        'time': 0,
                        'error': 'Invalid result format'
                    }
            except Exception as e:
                result = {
                    'status': 'ERROR',
                    'filename': filename,
                    'size': 0,
                    'time': 0,
                    'error': str(e)
                }
            results.append(result)
    return results

def run_single_stress_case(args):
    test_no, operation, size_label, client_pool, server_pool = args
    results = run_stress_test(operation, size_label, client_pool)
    success_count = sum(1 for r in results if r.get('status') == 'OK')
    fail_count = len(results) - success_count
    total_bytes = sum(r.get('size', 0) for r in results)
    total_time = sum(r.get('time', 0) for r in results if r.get('time', 0) > 0)
    throughput = total_bytes / total_time if total_time > 0 else 0

    row = [
        test_no, operation, size_label, client_pool, server_pool,
        round(total_time, 2), int(throughput),
        success_count, fail_count, success_count, fail_count
    ]
    print(f"Done test #{test_no} - {operation} {size_label} C:{client_pool} S:{server_pool}")
    return row

def main_stress_test(client_pool, server_pool):
    operations = ['upload', 'download']
    sizes = ['10MB', '50MB', '100MB']

    if not os.path.exists('stress_test_results.csv'):
        with open('stress_test_results.csv', 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow([
                'Test No', 'Operation', 'Size', 'Client Pool',
                'Server Pool', 'Total Time (s)', 'Throughput (B/s)',
                'Success Client', 'Fail Client', 'Success Server', 'Fail Server'
            ])

    test_args = []
    test_no = 1
    for size in sizes:
        for op in operations:
            test_args.append((test_no, op, size, client_pool, server_pool))
            test_no += 1

    for args in test_args:
        result = run_single_stress_case(args)
        with open('stress_test_results.csv', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(result)
            csvfile.flush()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Stress test client dan server file transfer")
    parser.add_argument('--client-pool', type=int, default=1,
                        help="Ukuran client pool, contoh: 1,5,50")
    parser.add_argument('--server-pool', type=int, default=1,
                        help="Ukuran server pool, contoh: 1,5,50")
    args = parser.parse_args()

    logging.basicConfig(level=logging.WARNING)
    main_stress_test(args.client_pool, args.server_pool)
