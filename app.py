import asyncio
import asyncpg
from aiohttp import web
from lib.logtaker import logger
import os
from dotenv import load_dotenv
import json
load_dotenv()


async def handle(request):
    """Handle incoming requests."""
    pool = request.app['pool']

    _code = request.rel_url.query.get('codes')
    _start_date = request.rel_url.query.get('start_date')
    _end_date = request.rel_url.query.get('end_date')

    codes = []
    for code in _code.split(','):
        codes.append(code)




    # Take a connection from the pool.
    async with pool.acquire() as connection:
        await connection.set_type_codec(
            'json',
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )
        # Open a transaction.
        async with connection.transaction():
            # Run the query passing the request argument.
            results = await connection.fetch(
                '''SELECT json_agg(json_build_object(
        'code', t.code,
        'd_change', t.d_change
    )) results  FROM stock_returns_by_given_dates_v3('{%s}', '%s', '%s') t;''' % (','.join(codes), _start_date, _end_date))

            logger.info(results[0].get('results'))
            return web.json_response(
                {'code': _code, 'start_date': _start_date, 'end_date': _end_date, 'result': results[0].get('results')})


async def init_app():
    """Initialize the application server."""
    app = web.Application()
    # Create a database connection pool
    app['pool'] = await asyncpg.create_pool(user=os.getenv('DB_USER'), password=os.getenv('DB_PASSWORD'),
                                            database=os.getenv('DB_NAME'), host=os.getenv('DB_HOST'),
                                            port=os.getenv('DB_PORT'))
    # Configure service routes
    app.router.add_route('GET', '/stock_returns', handle)
    return app


loop = asyncio.get_event_loop()
app = loop.run_until_complete(init_app())
web.run_app(app)
