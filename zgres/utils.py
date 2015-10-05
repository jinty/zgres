import sys
import asyncio
import logging

def exception_handler(loop, context):
    loop.default_exception_handler(context)
    logging.error('Unexpected exception, exiting...')
    # XXX: do we need to log the exception here?
    sys.exit(1)

def run_asyncio(*callback_and_args):
    loop = asyncio.get_event_loop()
    loop.set_exception_handler(exception_handler)
    if callback_and_args:
        loop.call_soon(*callback_and_args)
    loop.run_forever()
    logging.info('Exiting after being asked to stop nicely')
    return 0
