from aiohttp import web
from tasks import sand_mail_dummy_fun, get_status
from sqlalchemy import Column, String, Integer, DateTime, func
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

d_base = 'postgresql+asyncpg://postgres:postgres@localhost:5432/rat_data_base'
engine = create_async_engine(d_base)

Base = declarative_base()
Session = sessionmaker(bind=engine, class_=AsyncSession, expire_on_commit=False)


class Notifications(Base):
    __tablename__ = 'Notes'

    noti_id = Column(Integer, primary_key=True)
    header = Column(String)
    description = Column(String)
    creation_date = Column(DateTime, server_default=func.now())
    owner = Column(String, nullable=False)


async def app_context(app: web.Application):
    async with engine.begin() as dat_base_connect:
        async with Session() as session:
            await session.execute('CREATE EXTENSION IF NOT EXISTS "uuid-ossp"')
            await session.commit()
        await dat_base_connect.run_sync(Base.metadata.create_all)
    yield
    await engine.dispose()


async def rat_page(request):
    return web.json_response({'rat': 'rat'})


class Notis(web.View):

    async def get(self):
        async with Session() as session:
            noti_id = self.request.match_info['noti_id']
            responce = await session.get(Notifications, int(noti_id))
        return web.json_response({'success': {'owner': responce.owner,
                                              'header': responce.header,
                                              'description': responce.description,
                                              'creation_date': str(responce.creation_date)
                                              }
                                  })

    async def post(self):
        data = await self.request.json()
        async with Session() as session:
            session.add(Notifications(header=data['header'], description=data['description'], owner=data['owner']))
            await session.commit()
        return web.json_response({'success': 'note added'})

    async def patch(self):
        async with Session() as session:
            noti_id = self.request.match_info['noti_id']
            data = await self.request.json()
            obj = await session.get(Notifications, int(noti_id))
            for x, y in data.items():
                setattr(obj, x, y)
            await session.commit()
        return web.json_response({'success': 'note pathed'})

    async def delete(self):
        async with Session() as session:
            noti_id = self.request.match_info['noti_id']
            print(noti_id)
            obj = await session.get(Notifications, int(noti_id))
            print(obj)
            await session.delete(obj)
            await session.commit()
        return web.json_response({'success': 'deleted'})


class SandMail(web.View):
    async def post(self):
        resp = sand_mail_dummy_fun.delay()
        return web.json_response({'rat': resp.id})

    async def get(self):
        task_id = self.request.match_info['task_id']
        status = get_status(str(task_id))
        return web.json_response({'status': status})


app = web.Application()
app.cleanup_ctx.append(app_context)
app.router.add_get('', rat_page)
app.router.add_post('/notifications', Notis)
app.router.add_get('/notifications/{noti_id:\d+}', Notis)
app.router.add_patch('/notifications/{noti_id:\d+}', Notis)
app.router.add_delete('/notifications/{noti_id:\d+}', Notis)
app.router.add_post('/send', SandMail)
app.router.add_get('/task_status/{task_id}', SandMail)

if __name__ == '__main__':
    web.run_app(app)
