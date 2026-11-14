"""Local-only TCP bridge; keeps the kind node on one Docker network."""
import asyncio
import os
ROUTES={3306:("mysql",3306),9092:("kafka",9092),8080:("firestore",8080),4443:("gcs",4443),8085:("pubsub",8085),22181:("zookeeper-1",2181),22182:("zookeeper-2",2181),22183:("zookeeper-3",2181)}
async def bridge(reader,writer,target):
    other=None
    try:
        upstream,other=await asyncio.wait_for(asyncio.open_connection(*target),5)
        async def copy(source,destination):
            while data:=await source.read(65536):
                destination.write(data);await destination.drain()
        tasks=[asyncio.create_task(copy(reader,other)),asyncio.create_task(copy(upstream,writer))]
        _,pending=await asyncio.wait(tasks,return_when=asyncio.FIRST_COMPLETED)
        for task in pending:task.cancel()
        await asyncio.gather(*tasks,return_exceptions=True)
    except (OSError,asyncio.TimeoutError):pass
    finally:
        writer.close()
        if other:other.close()
async def main():
    if os.environ.get("APP_ENV")!="local":raise RuntimeError("This bridge is only for isolated local verification.")
    servers=[]
    for port,target in ROUTES.items():
        async def accept(reader,writer,destination=target):await bridge(reader,writer,destination)
        servers.append(await asyncio.start_server(accept,"0.0.0.0",port))
    print("Isolated local emulator bridge ready.",flush=True)
    await asyncio.gather(*(server.serve_forever() for server in servers))
if __name__=="__main__":asyncio.run(main())
