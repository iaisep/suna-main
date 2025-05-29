import httpx

# Monkey-patch default client behavior
_original_client = httpx.AsyncClient

class PatchedAsyncClient(httpx.AsyncClient):
    def __init__(self, *args, **kwargs):
        kwargs.setdefault("verify", False)
        super().__init__(*args, **kwargs)

httpx.AsyncClient = PatchedAsyncClient
