import sys
import time
import asyncio
import logging

def pg_lsn_to_int(pos):
    # http://www.postgresql.org/docs/9.4/static/datatype-pg-lsn.html
    # see http://eulerto.blogspot.com.es/2011/11/understanding-wal-nomenclature.html
    logfile, offset = pos.split('/')
    return 0xFF000000 * int(logfile, 16) + int(offset, 16)

def exception_handler(loop, context):
    loop.default_exception_handler(context)
    logging.error('Unexpected exception, exiting...')
    # sys.exit doesn't work very well, at least not in Python 3.5.0
    #   see: http://bugs.python.org/issue25489
    # so we take out insurance by later calling _stop()
    loop.call_soon(_stop, loop)
    sys.exit(1)

def _stop(loop):
    loop.stop()
    sys.exit(1)

def run_asyncio(*callback_and_args):
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)
    if callback_and_args:
        loop.call_soon(*callback_and_args)
    loop.run_forever()
    logging.info('Exiting after being asked to stop nicely')
    return 0
