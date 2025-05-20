import json
import asyncio
import time

HOST = '127.0.0.1'  
PORT = 6400
FILENAME="dump.rdb"

cache={}
lock={}
expirations={}

class Node:
    def __init__(self,value):
        self.value=value
        self.next=None

class List:
    def __init__(self):
        self.head=None
        self.tail=None
    
    def append(self,value):
        if self.tail is None:
            self.tail=Node(value)
            self.head=self.tail
        else:
            node=Node(value)
            self.tail.next=node
            self.tail=node
    
    def prepend(self,value):
        if self.head is None:
            self.head=Node(value)
            self.tail=self.head
        else:
            node=Node(value)
            node.next=self.head
            self.head=node

    def getlen(self):
        curr=self.head
        count=0
        while curr!=None:
            count+=1
            curr=curr.next
        return count
    
    def getRangeValues(self,start,end):
        index=start
        curr=self.head
        values=[]
        while curr != None and index<=end:
            values.append(curr.value)
            curr=curr.next
        return values

async def get_lock(key):
    if key not in lock:
        lock[key]=asyncio.Lock()
    return lock[key]

async def expire_key_after(key,ex_seconds):
    try:
        await asyncio.sleep(ex_seconds)
        cache.pop(key,None)
        expirations.pop(key,None)
    except Exception as e:
        print(e)

def key_exists(keys):
    count=0
    for key in keys:
        if key in cache:
            count+=1
    return f":{count}\r\n"

async def delete_key(keys):
    count=0
    for key in keys:
        if key in cache:
            async with await get_lock(key):
                cache.pop(key,None)
                count+=1
    return f":{count}\r\n"

async def increment_key(key):
    if key not in cache:
        async with await get_lock(key):
            cache[key]="1"
            return f":{cache[key]}\r\n"
    else:
        try:
            async with await get_lock(key):
                cache[key]=str(int(cache[key])+1)
                return f":{cache[key]}\r\n"
        except:
            return f"-ERR value is not an integer or out of range\r\n"

async def decrement_key(key):
    if key not in cache:
        async with await get_lock(key):
            cache[key]="-1"
            return f":{cache[key]}\r\n"
    else:
        try:
            async with await get_lock(key):
                cache[key]=str(int(cache[key])-1)
                return f":{cache[key]}\r\n"
        except:
            return f"-ERR value is not an integer or out of range\r\n"

async def lpush_values(key,values,lpush):
    async with await get_lock(key):
        if key not in cache:
            cache[key]=List()
        if lpush:
            for value in values:
                cache[key].prepend(value)
        else:
            for value in values:
                cache[key].append(value) 
        return f":{cache[key].getlen()}\r\n"

def save_contents(filename):
    with open(filename) as f:
        data={
            "cache":cache,
            "expirations":expirations
        }
        json.dump(data,f)
        return "+OK\r\n"

def load_contents(filename):
    with open(filename) as f:
        data=json.load(f)
        cache=data.get("cache",{})
        expirations=data.get("expirations",{})
        return cache,expirations
    return {},{}

def lrange(key,start,end):
    if key not in cache:
        return "*0\r\n\r\n"
    try:
        length=cache[key].getlen()
        if end<0:
            end=length+end
        values=cache[key].getRangeValues(start,end)
        returnString=f"*{len(values)}\r\n"
        for value in values:
            returnString+=f"${len(value)}\r\n{value}\r\n"
        return returnString
    except Exception as e:
        print(e)
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"

async def set_key(key,value,expiration_timer=None,exact_time=None,not_exists_condition=False,if_exists_condition=False):
    async with await get_lock(key):
        if not not_exists_condition and not if_exists_condition:
            if key in expirations:
                expirations[key].cancel()
            cache[key]=value
            if expiration_timer is not None:
                expirations[key]=asyncio.create_task(expire_key_after(key,expiration_timer))
            elif exact_time is not None:
                current_time=time.time()
                delay=exact_time-current_time
                if delay<=0:
                    cache.pop(key,None)
                    return "+OK (Key has already expired)\r\n"
                else:
                    if key in expirations:
                        expirations[key].cancel()
                        expirations[key]=asyncio.create_task(expire_key_after(key,delay))
        elif not_exists_condition:
            if key not in cache:
                if key in expirations:
                    expirations[key].cancel()
                cache[key]=value
                if expiration_timer is not None:
                    expirations[key]=asyncio.create_task(expire_key_after(key,expiration_timer))
                elif exact_time is not None:
                    current_time=time.time()
                    delay=exact_time-current_time
                    if delay<=0:
                        cache.pop(key)
                        return "+OK (Key has already expired)\r\n"
                    else:
                        if key in expirations:
                            expirations[key].cancel()
                            expirations[key]=asyncio.create_task(expire_key_after(key,delay))
        elif if_exists_condition:
            if key in cache:
                if key in expirations:
                    expirations[key].cancel()
                cache[key]=value
                if expiration_timer is not None:
                    expirations[key]=asyncio.create_task(expire_key_after(key,expiration_timer))
                elif exact_time is not None:
                    current_time=time.time()
                    delay=exact_time-current_time
                    if delay<=0:
                        cache.pop(key)
                        return "+OK (Key has already expired)\r\n"
                    else:
                        if key in expirations:
                            expirations[key].cancel()
                            expirations[key]=asyncio.create_task(expire_key_after(key,delay))
        return "+OK\r\n"

async def get_key(key):
    async with await get_lock(key):
        if key not in cache:
            return "$-1\r\n"
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
        return '-ERR Invalid command\r\n'
    
    command = args[0].upper()
    if command == 'PING':
        return '+PONG\r\n'
    elif command == 'ECHO' and len(args) == 2:
        arg = args[1]
        return f"${len(arg)}\r\n{arg}\r\n"
    elif command == 'COMMAND' and len(args) == 2 and args[1].upper() == 'DOCS':
        return '*0\r\n'
    elif command.upper()=='SET':
        if(len(args)==3):
            response=await set_key(args[1],args[2])
            return response
        elif (len(args)==4):
            if args[3].upper()=="XX":
                response=await set_key(args[1],args[2],if_exists_condition=True)
                return response
            elif args[3].upper()=="NX":
                response=await set_key(args[1],args[2],not_exists_condition=True)
                return response
            else:
                return "-ERR Invalid command syntax\r\n"
        elif len(args)==5:
            if args[3].upper()=='EX':
                response=await set_key(args[1],args[2],expiration_timer=float(args[4]))
                return response
            elif args[3].upper()=='PX':
                response=await set_key(args[1],args[2],expiration_timer=(float(args[4])/1000))
                return response
            elif args[3].upper()=='EXAT':
                response=await set_key(args[1],args[2],exact_time=float(args[4]))
                return response
            elif args[3].upper()=='PXAT':
                response=await set_key(args[1],args[2],exact_time=(float(args[4]))/1000)
                return response
            else:
                return "-ERR Invalid command syntax\r\n"
        elif len(args)==6:
            existence=args[3].upper()
            unix_flag=False
            if args[4].upper()=="EXAT":
                unix_flag=True
                time_value=float(args[5])
            elif args[4].upper()=="PXAT":
                unix_flag=True
                time_value=float(args[5])/1000
            elif args[4].upper()=="EX":
                time_value=float(args[5])
            elif args[4].upper()=="PX":
                time_value=float(args[5])/1000
            else:
                return "-ERR Invalid command syntax"
            if existence=="XX" and unix_flag:
                response=await set_key(args[1],args[2],exact_time=time_value,if_exists_condition=True)
            elif existence=="NX" and unix_flag:
                response=await set_key(args[1],args[2],exact_time=time_value,not_exists_condition=True)
            elif existence=="XX":
                response=await set_key(args[1],args[2],expiration_timer=time_value,if_exists_condition=True)
            elif existence=="NX":
                response=await set_key(args[1],args[2],expiration_timer=time_value,not_exists_condition=True)     
            return response         
            
    elif command.upper()=='GET' and len(args)==2:
        response=await get_key(args[1])
        return response
    elif command.upper()=="EXISTS" and len(args)>=2:
        keys=[args[index] for index in range(1,len(args))]
        response=key_exists(keys)
        return response
    elif command.upper()=="DEL" and len(args)>=2:
        keys=[args[index] for index in range(1,len(args))]
        response=await delete_key(keys)
        return response
    elif command.upper()=="ICR" and len(args)==2:
        response=await increment_key(args[1])
        return response
    elif command.upper()=="DCR" and len(args)==2:
        response=await decrement_key(args[1])
        return response
    elif command.upper()=="LPUSH" and len(args)>=3:
        values=[args[index] for index in range(2,len(args))]
        response=await lpush_values(args[1],values,True)
        return response
    elif command.upper()=="RPUSH" and len(args)>=3:
        values=[args[index] for index in range(2,len(args))]
        response=await lpush_values(args[1],values,False)
        return response
    elif command.upper()=="LRANGE" and len(args)==4:
        return lrange(args[1],int(args[2]),int(args[3]))
    else:
        return '-ERR unknown command\r\n'

async def handle_client(reader,writer):
    while True:
        data = await reader.read(1024)
        if not data:
            break
        commands = parse_response(data.decode())
        if(len(commands)==0):
            writer.write("-ERR Invalid command\r\n".encode())
        else:
            answer = await handle_command(commands)
            if answer is None:
                writer.write("-ERR Invalid command\r\n".encode())
            else:
                writer.write(answer.encode())
        await writer.drain()
    writer.close()
    await writer.wait_closed()

async def main():
    server=await asyncio.start_server(handle_client,host=HOST,port=PORT)
    addr=server.sockets[0].getsockname()
    print(f"Server listening on {addr}")
    async with server:
        await server.serve_forever()

if __name__=="__main__":
    asyncio.run(main())