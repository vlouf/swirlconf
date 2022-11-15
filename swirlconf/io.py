import pickle
import asyncio
import traceback


async def decode_message(message):
    data = pickle.loads(message)
    for key in ["who", "what", "where", "when", "uid"]:
        try:
            _ = data[key]
        except KeyError:
            raise KeyError(f"Key: {key} not found in incoming message.")

    return data


async def dispatch_message(message: str, port: int) -> None:
    """
    Handle outgoing connection.
    Dispatch message (assuming that the message is a valid radar file name) to
    the valid.

    Parameters:
    ===========
    message: str
        Message to send to the live service.
    """
    try:
        _, writer = await asyncio.open_connection("127.0.0.1", port)
        writer.write(message)
        writer.close()
    except ConnectionRefusedError:
        print(f"Could not send message to port {port}.")
        traceback.print_exc()

    return None
