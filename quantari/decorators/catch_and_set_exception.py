import logging


def catch_and_set_exception(func):
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            self = args[0]
            self.exception = True
            logging.error(f"Exception: {e}")

    return wrapper
