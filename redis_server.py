import socket

HOST = '127.0.0.1'  
PORT = 6400

cache={}

def parse_response(data):
    lines = data.split('\r\n')
    print("Parsed lines:", lines)
    if not lines or not lines[0].startswith('*'):
        return []
    
    num_commands = int(lines[0][1:])
    index = 1
    commands = []

    while index < len(lines) and len(commands) < num_commands:
        if not lines[index].startswith('$'):
            return []
        command_length = int(lines[index][1:])
        if index + 1 >= len(lines):
            return []
        command = lines[index + 1]
        commands.append(command)
        index += 2

    print(f"Extracted {len(commands)} commands:", commands)
    return commands

def handle_command(args):
    if not args:
        return '-ERROR\r\n'
    
    command = args[0].upper()
    if command == 'PING':
        return '+PONG\r\n'
    elif command == 'ECHO' and len(args) >= 2:
        arg = args[1]
        return f"${len(arg)}\r\n{arg}\r\n"
    elif command == 'COMMAND' and len(args) >= 2 and args[1].upper() == 'DOCS':
        return '*0\r\n'
    elif command.upper()=='SET' and len(args)>=3:
        attribute=args[1]
        value=args[2]
        cache[attribute]=value
        return "+OK\r\n"
    elif command.upper()=='GET' and len(args)>=2:
        if cache.get(args[1]) is None:
            return f'-ERR {args[1]} not found\r\n'
        else:
            return f'${len(cache[args[1]])}\r\n{cache[args[1]]}\r\n'
    else:
        return '-ERR unknown command\r\n'

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
    server_socket.bind((HOST, PORT))
    server_socket.listen(5)
    print(f"Server is listening on {HOST}:{PORT}")
    
    connection, addr = server_socket.accept()
    with connection:
        print(f"Connected to client {addr}")
        while True:
            response = connection.recv(1024)
            if not response:
                break
            print("Raw received:", response)
            commands = parse_response(response.decode())
            answer = handle_command(commands)
            print("Sending response:", answer.strip())
            connection.sendall(answer.encode())
