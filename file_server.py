import argparse
import logging
import socket
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from file_protocol import FileProtocol
fp = FileProtocol()

HOST = '0.0.0.0'
PORT = 7777

def handle_client(conn, addr):
    buffer = ''
    try:
        while True:
            data = conn.recv(65536)
            if not data:
                break
            buffer += data.decode()
            if '\r\n\r\n' in buffer:
                break
        if buffer.strip():
            command = buffer.strip('\r\n')
            logging.warning(f"data diterima dari {addr}: {command}")
            hasil = fp.proses_string(command)
            hasil += '\r\n\r\n'
            conn.sendall(hasil.encode())
    except Exception as e:
        logging.error(f"Terjadi kesalahan pada {addr}: {str(e)}")
    finally:
        conn.close()

def main(pool_sizes, executor):
    logging.basicConfig(level=logging.WARNING)
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((HOST, PORT))
    server_socket.listen(100)
    logging.warning(f"[PROCESS] Server running on {HOST}:{PORT}")

    if executor == 'process':
        with ProcessPoolExecutor(max_workers=pool_sizes) as pool:
            while True:
                conn, addr = server_socket.accept()
                pool.submit(handle_client, conn, addr)
    else:
        with ThreadPoolExecutor(max_workers=pool_sizes) as pool:
            while True:
                conn, addr = server_socket.accept()
                pool.submit(handle_client, conn, addr)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Number of Pool Server")
    parser.add_argument('--pool', type=int, default=1,
                        help="Ukuran server pool, contoh: 1,5,50")
    parser.add_argument('--operation', type=str, default='thread',
                    help="Jenis operasi: thread atau process")
    args = parser.parse_args()
    main(args.pool, args.operation)
