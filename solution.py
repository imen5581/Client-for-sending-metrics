import socket
import time


class ClientError(Exception):
    pass


class Client:

    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = port
        self.timeout = timeout
        timestam = int(time.time())

    def connect(self, data):
        try:
            answer = b''
            with socket.create_connection((self.host, self.port), self.timeout) as sock:
                sock.sendall(data.encode())
                portion = sock.recv(1024)
                try:
                    while portion:
                        answer += portion
                        portion = sock.recv(1024)
                except Exception as err:
                    if answer:
                        pass
                    else:
                        raise ClientError(err)

            result, values = answer.decode().split('\n', 1)
            if result == 'ok':
                return values.strip()
            else:
                raise ClientError(answer.decode())
        except Exception as err:
            raise ClientError(err)

    def put(self, name, value, timestamp = None):
        if timestamp is None:
            timestamp = int(time.time())
        send_string = 'put {0} {1} {2}\n'.format(name, str(value), str(timestamp))
        self.connect(send_string)

    def get(self, metric_name='*'):
        dict_data = dict()
        send_string = 'get {0}\n'.format(metric_name)
        data = self.connect(send_string)
        try:
            if data:
                for row in data.split('\n'):
                    key, value, timestamp = row.split()
                    if key not in dict_data:
                        dict_data[key] = []
                    dict_data[key].append((int(timestamp), float(value)))
                    dict_data[key].sort(key=lambda x: x[0])
        except ValueError as ve:
            raise ClientError(ve)
        return dict_data
#client = Client("127.0.0.1", 8888, timeout=15)
#client.put("palm.cpu", 0.5, timestamp=1150864247)
#print(client.get("*"))
