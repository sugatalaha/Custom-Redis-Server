import socket
import asyncio

lock={}

HOST = '127.0.0.1'  
PORT = 6400

cache={}

async def get_lock(key):
    if not key in lock:
        lock[key]=asyncio.Lock()
    return lock[key]

async def set_key(key,value):
    with get_lock(key):
        cache[key]=value
        return "+OK\r\n"

async def get_key(key):
    with get_lock(key):
        if key not in cache[key]:
            return f"-ERR {key} does not exist\r\n"
        else:
            return f"${len(cache[key])}\r\n{cache[key]}\r\n"

def parse_response(data):
    lines = data.split('\r\n')
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
    return commands

async def handle_command(args):
    if not args:
        return '-ERROR\r\n'
    
    command = args[0].upper()
    if command == 'PING':
        return '+PONG\r\n'
    elif command == 'ECHO' and len(args) == 2:
        arg = args[1]
        return f"${len(arg)}\r\n{arg}\r\n"
    elif command == 'COMMAND' and len(args) == 2 and args[1].upper() == 'DOCS':
        return '*0\r\n'
    elif command.upper()=='SET' and len(args)==3:
        response=await set_key(args[1],args[2])
        return response
    elif command.upper()=='GET' and len(args)==2:
        response=await get_key(args[1])
        return response
    else:
        return '-ERR unknown command\r\n'

async def main():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
        server_socket.bind((HOST, PORT))
        server_socket.listen(5)
        print(f"Server is listening on {HOST}:{PORT}")

        connection, addr = server_socket.accept()
        with connection:
            while True:
                response = connection.recv(1024)
                if not response:
                    break
                commands = parse_response(response.decode())
                answer = await handle_command(commands)
                connection.sendall(answer.encode())

if __name__=="__main__":
    asyncio.run(main())